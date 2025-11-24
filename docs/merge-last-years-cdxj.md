# Compact flat CDXJ files and convert to ZipNum

To compact flat CDXJ files to ZipNum and skipping some open collections CDXJ files.

Then compact flat cdxj files to ZipNum.

```bash
docker run --rm -v /data/indexes_cdx:/data/indexes_cdx:ro -v /data/replay_indexes_zipnum:/data/replay_indexes_zipnum arquivo/replay-cdxj-indexing-tools:latest bash -c "merge-flat-cdxj - /data/indexes_cdx/split/*.cdxj --exclude RAQ2025.cdxj --exclude PATCHING2025.cdxj --exclude SAWP4.cdxj --verbose 2> /data/replay_indexes_zipnum/merge-error.log | flat-cdxj-to-zipnum --idx-file awp.idx --loc-file awp.loc --base awp --workers 24  -i - -o /data/replay_indexes_zipnum"
```
