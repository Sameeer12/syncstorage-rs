initSidebarItems({"constant":[["SHA256",""],["SHA384",""],["SHA512",""]],"enum":[["DigestAlgorithm",""],["Error",""],["InvalidBewit",""]],"mod":[["crypto","`hawk` must perform certain cryptographic operations in order to function, and applications may need control over which library is used for these."],["mac",""]],"struct":[["Bewit","A Bewit is a piece of data attached to a GET request that functions in place of a Hawk Authentication header.  It contains an id, a timestamp, a MAC, and an optional `ext` value. These are available using accessor functions."],["Credentials","Hawk credentials: an ID and a key associated with that ID.  The digest algorithm must be agreed between the server and the client, and the length of the key is specific to that algorithm."],["Header","Representation of a Hawk `Authorization` header value (the part following \"Hawk \")."],["Key","Hawk key."],["PayloadHasher","A utility for hashing payloads. Feed your entity body to this, then pass the `finish` result to a request or response."],["Request","Request represents a single HTTP request."],["RequestBuilder",""],["Response","A Response represents a response from an HTTP server."],["ResponseBuilder",""]],"type":[["Result",""]]});