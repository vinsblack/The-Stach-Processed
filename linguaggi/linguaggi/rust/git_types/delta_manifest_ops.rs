/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

use std::iter::Iterator;
use std::sync::Arc;

use anyhow::Context;
use anyhow::Result;
use async_trait::async_trait;
use blobstore::Blobstore;
use blobstore::Loadable;
use bytes::Bytes;
use context::CoreContext;
use futures::StreamExt;
use futures::TryStreamExt;
use futures::stream::BoxStream;
use gix_hash::ObjectId;
use metaconfig_types::GitDeltaManifestVersion;
use mononoke_types::ChangesetId;
use mononoke_types::hash::GitSha1;
use mononoke_types::hash::RichGitSha1;
use mononoke_types::path::MPath;
use repo_derived_data::RepoDerivedData;

use crate::RootGitDeltaManifestV2Id;
use crate::delta_manifest_v2::GDMV2DeltaEntry;
use crate::delta_manifest_v2::GDMV2Entry;
use crate::delta_manifest_v2::GitDeltaManifestV2;
use crate::delta_manifest_v2::ObjectKind;

/// Fetches GitDeltaManifest for a given changeset with the given version.
/// Derives the GitDeltaManifest if not present.
pub async fn fetch_git_delta_manifest(
    ctx: &CoreContext,
    derived_data: &RepoDerivedData,
    blobstore: &impl Blobstore,
    git_delta_manifest_version: GitDeltaManifestVersion,
    cs_id: ChangesetId,
) -> Result<Box<dyn GitDeltaManifestOps + Send + Sync>> {
    match git_delta_manifest_version {
        GitDeltaManifestVersion::V2 => {
            let root_mf_id = derived_data
                .derive::<RootGitDeltaManifestV2Id>(ctx, cs_id)
                .await
                .with_context(|| {
                    format!(
                        "Error in deriving RootGitDeltaManifestV2Id for changeset {:?}",
                        cs_id
                    )
                })?;

            Ok(Box::new(
                root_mf_id
                    .manifest_id()
                    .load(ctx, blobstore)
                    .await
                    .with_context(|| {
                        format!(
                            "Error in loading GitDeltaManifestV2 from root id {:?}",
                            root_mf_id
                        )
                    })?,
            ))
        }
    }
}

/// Trait representing a version of GitDeltaManifest.
pub trait GitDeltaManifestOps {
    /// Returns a stream of the entries of the GitDeltaManifest. There should
    /// be an entry for each object at a path that differs from the corresponding
    /// object at the same path in one of the parents.
    fn into_entries<'a>(
        self: Box<Self>,
        ctx: &'a CoreContext,
        blobstore: &'a Arc<dyn Blobstore>,
    ) -> BoxStream<'a, Result<(MPath, Box<dyn GitDeltaManifestEntryOps + Send>)>>;
}

impl GitDeltaManifestOps for GitDeltaManifestV2 {
    fn into_entries<'a>(
        self: Box<Self>,
        ctx: &'a CoreContext,
        blobstore: &'a Arc<dyn Blobstore>,
    ) -> BoxStream<'a, Result<(MPath, Box<dyn GitDeltaManifestEntryOps + Send>)>> {
        GitDeltaManifestV2::into_entries(*self, ctx, blobstore)
            .map_ok(
                |(path, entry)| -> (_, Box<dyn GitDeltaManifestEntryOps + Send>) {
                    (path, Box::new(entry))
                },
            )
            .boxed()
    }
}

/// Trait representing a subentry of a GitDeltaManifest.
pub trait GitDeltaManifestEntryOps {
    /// Returns the size of the full object.
    fn full_object_size(&self) -> u64;

    /// Returns the OID of the full object.
    fn full_object_oid(&self) -> ObjectId;

    /// Returns the kind of the full object.
    fn full_object_kind(&self) -> ObjectKind;

    /// Returns the RichGitSha1 of the full object.
    fn full_object_rich_git_sha1(&self) -> Result<RichGitSha1> {
        let sha1 = GitSha1::from_bytes(self.full_object_oid().as_bytes())?;
        let ty = match self.full_object_kind() {
            ObjectKind::Blob => "blob",
            ObjectKind::Tree => "tree",
        };
        Ok(RichGitSha1::from_sha1(sha1, ty, self.full_object_size()))
    }

    fn full_object_inlined_bytes(&self) -> Option<Bytes>;

    /// Returns an iterator over the deltas of the subentry.
    fn deltas(&self) -> Box<dyn Iterator<Item = &(dyn ObjectDeltaOps + Sync)> + '_>;
}

impl GitDeltaManifestEntryOps for GDMV2Entry {
    fn full_object_size(&self) -> u64 {
        self.full_object.size
    }

    fn full_object_oid(&self) -> ObjectId {
        self.full_object.oid
    }

    fn full_object_kind(&self) -> ObjectKind {
        self.full_object.kind
    }

    fn full_object_inlined_bytes(&self) -> Option<Bytes> {
        self.full_object.inlined_bytes.clone()
    }

    fn deltas(&self) -> Box<dyn Iterator<Item = &(dyn ObjectDeltaOps + Sync)> + '_> {
        Box::new(
            self.deltas
                .iter()
                .map(|delta| delta as &(dyn ObjectDeltaOps + Sync)),
        )
    }
}

/// Trait representing a delta in a GitDeltaManifest.
#[async_trait]
pub trait ObjectDeltaOps {
    /// Returns the uncompressed size of the instructions.
    fn instructions_uncompressed_size(&self) -> u64;

    /// Returns the compressed size of the instructions.
    fn instructions_compressed_size(&self) -> u64;

    /// Returns the OID of the base object.
    fn base_object_oid(&self) -> ObjectId;

    /// Returns the path of the base object.
    fn base_object_path(&self) -> &MPath;

    /// Returns the kind of the base object.
    fn base_object_kind(&self) -> ObjectKind;

    /// Returns the size of the base object in bytes.
    fn base_object_size(&self) -> u64;

    /// Returns the instructions bytes of the delta.
    async fn instruction_bytes(
        &self,
        ctx: &CoreContext,
        blobstore: &Arc<dyn Blobstore>,
        cs_id: ChangesetId,
        path: MPath,
    ) -> Result<Bytes>;
}

#[async_trait]
impl ObjectDeltaOps for GDMV2DeltaEntry {
    fn instructions_uncompressed_size(&self) -> u64 {
        self.instructions.uncompressed_size
    }

    fn instructions_compressed_size(&self) -> u64 {
        self.instructions.compressed_size
    }

    fn base_object_oid(&self) -> ObjectId {
        self.base_object.oid
    }

    fn base_object_path(&self) -> &MPath {
        &self.base_object_path
    }

    fn base_object_kind(&self) -> ObjectKind {
        self.base_object.kind
    }

    fn base_object_size(&self) -> u64 {
        self.base_object.size
    }

    async fn instruction_bytes(
        &self,
        ctx: &CoreContext,
        blobstore: &Arc<dyn Blobstore>,
        _cs_id: ChangesetId,
        _path: MPath,
    ) -> Result<Bytes> {
        self.instructions
            .instruction_bytes
            .clone()
            .into_raw_bytes(ctx, blobstore)
            .await
    }
}
