use anyhow::{Context, bail};
use karakeep_client::{BookmarkCreate, KarakeepClient};
use reqwest::{Client, header};
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{collections::HashSet, time::Duration};

const LISTS_QUERY: &str = r#"
query StarLists($after: String) {
  viewer {
    lists(first: 100, after: $after) {
      nodes { id name }
      pageInfo { hasNextPage endCursor }
    }
  }
}
"#;

const ITEMS_QUERY: &str = r#"
query StarListItems($listId: ID!, $after: String) {
  node(id: $listId) {
    ... on UserList {
      items(first: 100, after: $after) {
        nodes { ... on Repository { url nameWithOwner } }
        pageInfo { hasNextPage endCursor }
      }
    }
  }
}
"#;

#[derive(Deserialize)]
struct GraphqlResponse<T> {
    data: Option<T>,
    #[serde(default)]
    errors: Vec<Value>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PageInfo {
    has_next_page: bool,
    end_cursor: Option<String>,
}

impl PageInfo {
    fn next_cursor(self, seen: &mut HashSet<String>) -> anyhow::Result<Option<String>> {
        if !self.has_next_page {
            return Ok(None);
        }
        let cursor = self
            .end_cursor
            .filter(|c| !c.is_empty())
            .context("GitHub returned an empty pagination cursor")?;
        if !seen.insert(cursor.clone()) {
            bail!("GitHub returned an invalid pagination cursor");
        }
        Ok(Some(cursor))
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Connection<T> {
    nodes: Vec<Option<T>>,
    page_info: PageInfo,
}

#[derive(Deserialize)]
struct StarList {
    id: String,
    name: String,
}

#[derive(Deserialize)]
struct ListsData {
    viewer: ListOwner,
}

#[derive(Deserialize)]
struct ListOwner {
    lists: Connection<StarList>,
}

#[derive(Deserialize)]
struct ItemsData {
    node: Option<ListItems>,
}

#[derive(Deserialize)]
struct ListItems {
    items: Connection<Repository>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Repository {
    url: Option<String>,
    name_with_owner: Option<String>,
}

pub(super) struct GithubLists {
    client: Client,
    endpoint: String,
}

impl GithubLists {
    pub(super) fn new(token: &str, endpoint: &str) -> anyhow::Result<Self> {
        let mut headers = header::HeaderMap::new();
        let mut auth = header::HeaderValue::from_str(&format!("Bearer {token}"))?;
        auth.set_sensitive(true);
        headers.insert(header::AUTHORIZATION, auth);
        let client = Client::builder()
            .default_headers(headers)
            .user_agent("karakeep-sync/0.1")
            .timeout(Duration::from_secs(30))
            .build()?;
        Ok(Self {
            client,
            endpoint: endpoint.into(),
        })
    }

    async fn query<T: DeserializeOwned>(&self, query: &str, variables: Value) -> anyhow::Result<T> {
        let response = self
            .client
            .post(&self.endpoint)
            .json(&json!({ "query": query, "variables": variables }))
            .send()
            .await?
            .error_for_status()?
            .json::<GraphqlResponse<T>>()
            .await?;
        // GraphQL can return HTTP 200 with partial data and authorization errors.
        if !response.errors.is_empty() {
            bail!("GitHub list query failed: {}", json!(response.errors));
        }
        response.data.context("GitHub list query returned no data")
    }

    pub(super) async fn sync(&self, karakeep: &KarakeepClient) -> anyhow::Result<()> {
        let mut after = None;
        let mut seen = HashSet::new();
        let mut list_count = 0;
        let mut membership_count = 0;
        loop {
            let page: ListsData = self.query(LISTS_QUERY, json!({ "after": after })).await?;
            let parent_id = karakeep.ensure_list_exists("GitHub Starred").await?;
            for list in page.viewer.lists.nodes.into_iter().flatten() {
                let name = list.name;
                let list_id = karakeep.ensure_child_list_exists(&name, &parent_id).await?;
                let mut items_after = None;
                let mut items_seen = HashSet::new();
                loop {
                    let page: ItemsData = self
                        .query(
                            ITEMS_QUERY,
                            json!({ "listId": list.id, "after": items_after }),
                        )
                        .await?;
                    let items = page
                        .node
                        .context("GitHub star list is no longer accessible")?
                        .items;
                    for repo in items.nodes.into_iter().flatten() {
                        // Deleted or non-repository items have no repository fields.
                        let (Some(url), Some(title)) = (repo.url, repo.name_with_owner) else {
                            continue;
                        };
                        let bookmark = BookmarkCreate {
                            title,
                            url,
                            created_at: None,
                        };
                        karakeep
                            .upsert_bookmark_to_list(&bookmark, &list_id)
                            .await?;
                        membership_count += 1;
                    }
                    items_after = items.page_info.next_cursor(&mut items_seen)?;
                    if items_after.is_none() {
                        break;
                    }
                }
                list_count += 1;
                tracing::info!("synced GitHub star list: {}", name);
            }
            after = page.viewer.lists.page_info.next_cursor(&mut seen)?;
            if after.is_none() {
                break;
            }
        }
        tracing::info!(
            "GitHub lists sync complete (lists={}, memberships={})",
            list_count,
            membership_count
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{body_partial_json, method, path, query_param},
    };

    fn connection(nodes: Value, next: Option<&str>) -> Value {
        json!({ "nodes": nodes, "pageInfo": { "hasNextPage": next.is_some(), "endCursor": next } })
    }

    async fn graphql_page(server: &MockServer, variables: Value, data: Value, times: u64) {
        Mock::given(method("POST"))
            .and(path("/graphql"))
            .and(body_partial_json(json!({
                "query": if variables.get("listId").is_some() { ITEMS_QUERY } else { LISTS_QUERY },
                "variables": variables
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "data": data })))
            .expect(times)
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn paginates_lists_and_items_and_backfills_existing_bookmarks_idempotently() {
        let server = MockServer::start().await;
        graphql_page(
            &server,
            json!({"after": null}),
            json!({"viewer": {"lists": connection(json!([
            {"id": "one", "name": "Tools"}
        ]), Some("lists-2"))}}),
            2,
        )
        .await;
        graphql_page(
            &server,
            json!({"after": "lists-2"}),
            json!({"viewer": {"lists": connection(json!([
            {"id": "two", "name": "Reading"}, {"id": "empty", "name": "Empty"}
        ]), None)}}),
            2,
        )
        .await;

        let repos: Vec<Value> = (0..6).map(|i| json!({
            "url": format!("https://github.com/owner/repo{i}"), "nameWithOwner": format!("owner/repo{i}")
        })).collect();
        graphql_page(
            &server,
            json!({"listId": "one", "after": null}),
            json!({"node": {
                "items": connection(json!(repos), Some("items-2"))
            }}),
            2,
        )
        .await;
        graphql_page(&server, json!({"listId": "one", "after": "items-2"}), json!({"node": {
            "items": connection(json!([null, {}, {"url": "https://github.com/owner/repo6", "nameWithOwner": "owner/repo6"}]), None)
        }}), 2).await;
        graphql_page(&server, json!({"listId": "two", "after": null}), json!({"node": {
            "items": connection(json!([{"url": "https://github.com/owner/repo0", "nameWithOwner": "owner/repo0"}]), None)
        }}), 2).await;
        graphql_page(
            &server,
            json!({"listId": "empty", "after": null}),
            json!({"node": {
                "items": connection(json!([]), None)
            }}),
            2,
        )
        .await;

        Mock::given(method("GET"))
            .and(path("/api/v1/lists"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"lists": [
                {"id": "all", "name": "GitHub Starred", "parentId": null},
                {"id": "tools", "name": "Tools", "parentId": "all"},
                {"id": "reading", "name": "Reading", "parentId": "all"},
                {"id": "empty", "name": "Empty", "parentId": "all"},
                {"id": "unrelated", "name": "Tools", "parentId": null}
            ]})))
            .mount(&server)
            .await;
        for i in 0..7 {
            let url = format!("https://github.com/owner/repo{i}");
            Mock::given(method("GET"))
                .and(path("/api/v1/bookmarks/search"))
                .and(query_param("q", &url))
                .respond_with(
                    ResponseTemplate::new(200).set_body_json(json!({"bookmarks": [
                        {"id": format!("b{i}"), "content": {"url": url}}
                    ]})),
                )
                .expect(if i == 0 { 4 } else { 2 })
                .mount(&server)
                .await;
            Mock::given(method("PUT"))
                .and(path(format!("/api/v1/lists/tools/bookmarks/b{i}")))
                .respond_with(ResponseTemplate::new(204))
                .expect(2)
                .mount(&server)
                .await;
        }
        Mock::given(method("PUT"))
            .and(path("/api/v1/lists/reading/bookmarks/b0"))
            .respond_with(ResponseTemplate::new(204))
            .expect(2)
            .mount(&server)
            .await;
        // All repositories already exist. Repeated imports must not create duplicates or remove memberships.
        for verb in ["POST", "DELETE"] {
            Mock::given(method(verb))
                .and(path("/api/v1/bookmarks"))
                .respond_with(ResponseTemplate::new(500))
                .expect(0)
                .mount(&server)
                .await;
        }
        let github = GithubLists::new("test-token", &format!("{}/graphql", server.uri())).unwrap();
        let karakeep = KarakeepClient::new(&server.uri(), "test-token");
        github.sync(&karakeep).await.unwrap();
        github.sync(&karakeep).await.unwrap();
        server.verify().await;
    }

    #[tokio::test]
    async fn creates_children_under_the_requested_parent_without_reusing_names_elsewhere() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/lists"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"lists": [
                {"id": "root", "name": "GitHub Starred", "parentId": null},
                {"id": "wrong-root", "name": "GitHub Starred", "parentId": "elsewhere"},
                {"id": "top-level", "name": "Tools", "parentId": null},
                {"id": "unrelated", "name": "Tools", "parentId": "elsewhere"}
            ]})))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v1/lists"))
            .and(body_partial_json(
                json!({"name": "Tools", "parentId": "root"}),
            ))
            .respond_with(ResponseTemplate::new(201).set_body_json(json!({"id": "child"})))
            .expect(1)
            .mount(&server)
            .await;
        let karakeep = KarakeepClient::new(&server.uri(), "test-token");
        assert_eq!(
            karakeep.ensure_list_exists("GitHub Starred").await.unwrap(),
            "root"
        );
        assert_eq!(
            karakeep
                .ensure_child_list_exists("Tools", "root")
                .await
                .unwrap(),
            "child"
        );
    }

    #[tokio::test]
    async fn rejects_graphql_errors_even_with_partial_data() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": {"viewer": {"lists": connection(json!([]), None)}},
                "errors": [{"message": "Insufficient permissions"}]
            })))
            .mount(&server)
            .await;
        let github = GithubLists::new("test-token", &server.uri()).unwrap();
        let error = github
            .sync(&KarakeepClient::new(&server.uri(), "test-token"))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("Insufficient permissions"));
    }

    #[tokio::test]
    async fn rejects_http_errors() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(403))
            .mount(&server)
            .await;
        let github = GithubLists::new("test-token", &server.uri()).unwrap();
        assert!(
            github
                .sync(&KarakeepClient::new(&server.uri(), "test-token"))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn reports_failed_membership_writes() {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .respond_with(ResponseTemplate::new(403))
            .mount(&server)
            .await;
        let karakeep = KarakeepClient::new(&server.uri(), "test-token");
        assert!(
            karakeep
                .ensure_bookmark_in_list("bookmark", "list")
                .await
                .is_err()
        );
    }

    #[test]
    fn rejects_missing_and_repeated_cursors() {
        for cursor in [None, Some("same".into()), Some("".into())] {
            let page = PageInfo {
                has_next_page: true,
                end_cursor: cursor,
            };
            assert!(
                page.next_cursor(&mut HashSet::from(["same".into()]))
                    .is_err()
            );
        }
    }

    #[test]
    fn rejects_pagination_cycles() {
        let mut seen = HashSet::new();
        for cursor in ["a", "b"] {
            let page = PageInfo {
                has_next_page: true,
                end_cursor: Some(cursor.into()),
            };
            assert_eq!(
                page.next_cursor(&mut seen).unwrap().as_deref(),
                Some(cursor)
            );
        }
        let page = PageInfo {
            has_next_page: true,
            end_cursor: Some("a".into()),
        };
        assert!(page.next_cursor(&mut seen).is_err());
    }
}
