// Copyright 2021 Jannis Harder
// Copyright 2016 Mozilla Foundation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::cache::{Cache, CacheRead, CacheWrite, Storage};
use crate::errors::*;
use std::io::Cursor;
use std::time::{Duration, Instant};
use uuid::Uuid;

/// Blake3 hash of "sccache gha v1"
static KEY_SPACE: &str = "ec327fb452080d07d2c893885ea7759eeea5a36891273aebd6ef0677a99dc0eb";

/// A cache that stores entries in the GitHub Actions Cache.
pub struct GitHubActionsCache {
    client: rust_actions_cache_api::Cache,
}

impl GitHubActionsCache {
    /// Creates a new `GitHubActionsCache` implicitly configured using the environment provided by
    /// GitHub Actions' runner.
    pub fn new() -> Result<Self> {
        Ok(Self {
            client: rust_actions_cache_api::Cache::new()?,
        })
    }
}

#[async_trait]
impl Storage for GitHubActionsCache {
    async fn get(&self, key: &str) -> Result<Cache> {
        if let Some((_hit, blob)) = self.client.get_bytes(KEY_SPACE, &[key]).await? {
            CacheRead::from(Cursor::new(blob)).map(Cache::Hit)
        } else {
            Ok(Cache::Miss)
        }
    }

    async fn put(&self, key: &str, entry: CacheWrite) -> Result<Duration> {
        let start = Instant::now();
        // Overwriting existing entries is not possible, but GHA's key lookup is a prefix lookup
        // returning the newest matching entry, so by appending a UUID when storing, we effectively
        // get the intended overwrite behavior.

        // Eviction is handled automatically by GHA's cache.
        self.client
            .put_bytes(
                KEY_SPACE,
                &format!("{}-{}", key, Uuid::new_v4()),
                entry.finish()?.into(),
            )
            .await?;
        Ok(start.elapsed())
    }

    fn location(&self) -> String {
        "GitHub Actions Cache".into()
    }

    async fn current_size(&self) -> Result<Option<u64>> {
        Ok(None)
    }

    async fn max_size(&self) -> Result<Option<u64>> {
        Ok(None)
    }
}
