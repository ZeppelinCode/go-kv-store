# Simple Document Store

This is an experiment to create a simple key-value document
store that is indexed and persisted on disk.

## Usage

```sh
cd src
go run main.go
```

### Api
```
List all documents
GET http://localhost:3000/documents

Get a document
GET http://localhost:3000/documents/:document-name

Create new document
POST http://localhost:3000/documents
Body:
{
  "name": $documentName,
  "payload": $someString

}

Update a document
PUT http://localhost:3000/documents/:document-name
Body:
{
  "payload': $someString
}

Delete a document
DELETE http://localhost:3000/documents/:document-name
```

## Storage
The docuemnt store is backed by three (hardcoded) files:

* dbstore : contains the contents of all documents
* dbstore_i : index for the dbstore
* dbstore_f : keeps track of the free spaces in the document

## Data model

### Documents (dbstore)

Each document is represented as a liked list of data blocks where each data block has the following format


| 8 bytes  | 8 bytes | 8 Bytes | $DataLen bytes |
|--------- | ------- | ------- | -------------- |
| DataLen  | PrevLoc | NextLoc | Data           | 

### Index (dbstore_i)

Each document is indexed for quicker retrieval. The index file
contains a comma-separated list of document name to document
location map.

`$doc_name:$doc_location,$doc_name2:$doc_location2,...`

### Free spaces index (dbstore_f)
Gaps are created when documents are deleted. Document deletion
does not delete the contents from dbstore, it simply marks the freed up space as reusable.

```
1. dbStore: doc1(100 bytes) -> doc2(100 bytes) -> doc3(100 bytes)
2. Delete doc2
3. dbStore: doc1(100 bytes) -> GAP(100 bytes) -> doc3(100 bytes)
4. Insert doc4(50 bytes)
5. dbStore: doc1(100 bytes) -> doc4(50 bytes) -> GAP(50 bytes) -> doc3(100 bytes)
```

Free spaces are tracked in the following format:

| 8 bytes  | 8 bytes | 8 Bytes  | 8 Bytes | .... |
|--------- | ------- | -------  | --------| ---- |
| Position | Len     | Position | Len     | .... |

## Limitations

Document names can't contain:
* spaces (no http encoding)
* `,` or `:` (reserved for index)

Multi-threaded inserts/updates/deletes are likely to cause problems.