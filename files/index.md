**NOTE: We're moving to GitHub! Pardon our dust as we move things over.**
- fotinakis, 2013-05-01

----

Test!

```python
from titan import files

titan_file = files.File('/path/to/file.html')

titan_file.Write('Some content')
titan_file.Delete()
titan_file.CopyTo(destination_file=files.File('/other/file.html'))
titan_file.MoveTo(destination_file=files.File('/other/file.html'))
titan_file.Serialize()
titan_file.ValidatePath('/some/path')

titan_file.name         # "file.html"
titan_file.name_clean   # "file"
titan_file.extension    # ".html"
titan_file.path         # "/path/to/file.html"
titan_file.paths        # ["/", "/path", "/path/to"]
titan_file.mime_type    # "text/html"
titan_file.created      # datetime.datetime object.
titan_file.modified     # datetime.datetime object.
titan_file.content      # 'Some content'
titan_file.blob         # blobstore.BlobInfo object.
titan_file.exists       # Boolean.
titan_file.created_by   # titan.users.TitanUser object.
titan_file.modified_by  # titan.users.TitanUser object.
titan_file.size         # Integer number of bytes.
titan_file.md5_hash     # Pre-computed md5_hash of the file contents.
titan_file.meta         # Metadata object.
```

