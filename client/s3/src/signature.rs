use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::str;
use chrono::{DateTime, Utc, NaiveDate};
use digest::Digest;
use hex;
use hmac::{Hmac, Mac, NewMac};
use md5::Md5;
use base64;
use percent_encoding::{utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
use sha2::Sha256;


/// Payload string to use for signed empty payload
pub static EMPTY_SHA256_HASH: &str =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// Payload string to use for unsigned payload
pub static UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";

/// Do not URI-encode any of the unreserved characters that RFC 3986 defines:
/// A-Z, a-z, 0-9, hyphen ( - ), underscore ( _ ), period ( . ), and tilde ( ~ ).
/// Percent-encode all other characters with %XY, where X and Y are hexadecimal
/// characters (0-9 and uppercase A-F). For example, the space character must be
/// encoded as %20 (not using '+', as some encoding schemes do) and extended UTF-8
/// characters must be in the form %XY%ZA%BC
/// This constant is used to maintain the strict URI encoding standard as proposed by RFC 3986
pub const STRICT_ENCODE_SET: AsciiSet = NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

/// This struct is used to maintain the URI path encoding
pub const STRICT_PATH_ENCODE_SET: AsciiSet = STRICT_ENCODE_SET.remove(b'/');

pub type Params = BTreeMap<String, Option<String>>;

#[derive(Clone, Default)]
pub struct AwsCredentials {
    pub key: String,
    pub secret: String,
}

impl AwsCredentials {
    /// Create a new `AwsCredentials` from a key ID, secret key, optional access token, and expiry
    pub fn new(key: &str, secret: &str) -> AwsCredentials {
        AwsCredentials {
            key: key.to_string(),
            secret: secret.to_string(),
        }
    }

    /// Get a reference to the access key ID.
    pub fn aws_access_key_id(&self) -> &str {
        &self.key
    }

    /// Get a reference to the secret access key.
    pub fn aws_secret_access_key(&self) -> &str {
        &self.secret
    }
}

#[inline]
#[doc(hidden)]
pub fn encode_uri_path(uri: &str) -> String {
    utf8_percent_encode(uri, &STRICT_PATH_ENCODE_SET).collect::<String>()
}

#[inline]
fn encode_uri_strict(uri: &str) -> String {
    utf8_percent_encode(uri, &STRICT_ENCODE_SET).collect::<String>()
}

/// Canonicalizes query while iterating through the given paramaters
///
/// Read more about it: [HERE](http://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html#query-string-auth-v4-signing)
fn build_canonical_query_string(params: &Params) -> String {
    if params.is_empty() {
        return String::new();
    }

    let mut output = String::new();
    for (key, val) in params.iter() {
        if !output.is_empty() {
            output.push_str("&");
        }
        output.push_str(&encode_uri_strict(&key));
        output.push_str("=");

        if let Some(ref unwrapped_val) = *val {
            output.push_str(&encode_uri_strict(&unwrapped_val));
        }
    }

    output
}

fn to_hexdigest<T: AsRef<[u8]>>(t: T) -> String {
    let h = Sha256::digest(t.as_ref());
    hex::encode(h)
}

/// Convert payload from Char array to useable <payload, len> format.
fn digest_payload(payload: &[u8]) -> (String, usize) {
    let digest = to_hexdigest(payload);
    let len = payload.len();
    (digest, len)
}

fn skipped_headers(header: &str) -> bool {
    ["authorization", "content-length", "user-agent"].contains(&header)
}

fn signed_headers(headers: &BTreeMap<String, Vec<Vec<u8>>>) -> String {
    let mut signed = String::new();
    headers
        .iter()
        .filter(|&(ref key, _)| !skipped_headers(&key))
        .for_each(|(key, _)| {
            if !signed.is_empty() {
                signed.push(';');
            }
            signed.push_str(key);
        });
    signed
}

/// Canonicalizes values into the AWS Canonical Form.
///
/// Read more about it: [HERE](http://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html)
fn canonical_values(values: &[Vec<u8>]) -> String {
    let mut st = String::new();
    for v in values {
        let s = str::from_utf8(v).unwrap();
        if !st.is_empty() {
            st.push(',')
        }
        if s.starts_with('\"') {
            st.push_str(s);
        } else {
            st.push_str(s.replace("  ", " ").trim());
        }
    }
    st
}

/// Canonicalizes headers into the AWS Canonical Form.
///
/// Read more about it: [HERE](http://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html)
fn canonical_headers(headers: &BTreeMap<String, Vec<Vec<u8>>>) -> String {
    let mut canonical = String::new();

    for (key, value) in headers.iter() {
        if skipped_headers(key) {
            continue;
        }
        canonical.push_str(format!("{}:{}\n", key, canonical_values(value)).as_ref());
    }
    canonical
}

#[inline]
fn hmac(secret: &[u8], message: &[u8]) -> Hmac<Sha256> {
    let mut hmac = Hmac::<Sha256>::new_varkey(secret).expect("failed to create hmac");
    hmac.update(message);
    hmac
}

/// Mark string as AWS4-HMAC-SHA256 hashed
pub fn string_to_sign(date: DateTime<Utc>, hashed_canonical_request: &str, scope: &str) -> String {
    format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        date.format("%Y%m%dT%H%M%SZ"),
        scope,
        hashed_canonical_request
    )
}

/// Takes a message and signs it using AWS secret, time, region keys and service keys.
fn sign_string (
    string_to_sign: &str,
    secret: &str,
    date: NaiveDate,
    region: &str,
    service: &str,
) -> String {
    let date_str = date.format("%Y%m%d").to_string();
    let date_hmac = hmac(format!("AWS4{}", secret).as_bytes(), date_str.as_bytes())
        .finalize()
        .into_bytes();
    let region_hmac = hmac(date_hmac.as_ref(), region.as_bytes())
        .finalize()
        .into_bytes();
    let service_hmac = hmac(region_hmac.as_ref(), service.as_bytes())
        .finalize()
        .into_bytes();
    let signing_hmac = hmac(service_hmac.as_ref(), b"aws4_request")
        .finalize()
        .into_bytes();
    hex::encode(
        hmac(signing_hmac.as_ref(), string_to_sign.as_bytes())
            .finalize()
            .into_bytes(),
    )
}

#[derive(Debug)]
pub struct SignedRequest {
    /// The HTTP Method
    pub method: String,
    /// The AWS Service
    pub service: String,
    /// The AWS Region
    pub region: String,
    /// The HTTP Request path
    pub path: String,
    /// The HTTP Request Headers
    pub headers: BTreeMap<String, Vec<Vec<u8>>>,
    /// The HTTP Request paramaters
    pub params: BTreeMap<String, Option<String>>,
    /// The HTTP/HTTPS protocol
    pub scheme: Option<String>,
    /// The AWS hostname
    pub hostname: String,
    /// The HTTP Content
    pub payload: Option<&'static Vec<u8>>,
    // The Standardised query string
    pub canonical_query_string: String,
    /// The Standardised URI
    pub canonical_uri: String,
}

impl SignedRequest {
    /// Default constructor
    pub fn new(method: &str, service: &str, region: &str, path: &str, host: &str) -> SignedRequest {
        SignedRequest {
            method: method.to_string(),
            service: service.to_string(),
            region: region.to_string(),
            path: path.to_string(),
            headers: BTreeMap::new(),
            params: Params::new(),
            scheme: None,
            hostname: host.to_string(),
            payload: None,
            canonical_query_string: String::new(),
            canonical_uri: String::new(),
        }
    }

    /// Invokes `canonical_uri(path)` to return a canonical path
    pub fn canonical_path(&self) -> String {
        encode_uri_path(&self.path)
    }

    /// Converts a paramater such as "example param": "examplekey" into "&example+param=examplekey"
    pub fn canonical_query_string(&self) -> &str {
        &self.canonical_query_string
    }

    /// If the key exists in headers, set it to blank/unoccupied:
    pub fn remove_header(&mut self, key: &str) {
        let key_lower = key.to_ascii_lowercase();
        self.headers.remove(&key_lower);
    }

    /// Add a value to the array of headers for the specified key.
    /// Headers are kept sorted by key name for use at signing (BTreeMap)
    pub fn add_header<K: ToString>(&mut self, key: K, value: &str) {
        let mut key_lower = key.to_string();
        key_lower.make_ascii_lowercase();

        let value_vec = value.as_bytes().to_vec();

        self.headers.entry(key_lower).or_default().push(value_vec);
    }

     /// Returns the current HTTP method
     pub fn method(&self) -> &str {
        &self.method
    }

    /// Modify the region used for signing if needed, such as for AWS Organizations
    pub fn region_for_service(&self) -> String {
        self.region.to_string()
    }

    /// Returns the current http scheme (https or http)
    pub fn scheme(&self) -> String {
        "http".to_owned()
    }

    /// Adds parameter to the HTTP Request
    pub fn add_param<S>(&mut self, key: S, value: S)
    where
        S: Into<String>,
    {
        self.params.insert(key.into(), Some(value.into()));
    }

    /// Complement SignedRequest by ensuring the following HTTP headers are set accordingly:
    /// - host
    /// - content-type
    /// - content-length (if applicable)
    pub fn complement(&mut self) {
        // build the canonical request
        self.canonical_uri = self.canonical_path();
        self.canonical_query_string = build_canonical_query_string(&self.params);

        // if there's no content-type header set, set it to the default value
        if let Entry::Vacant(entry) = self.headers.entry("content-type".to_owned()) {
            let mut values = Vec::new();
            values.push(b"application/octet-stream".to_vec());
            entry.insert(values);
        }
    }

    /// Signs the request using Amazon Signature v4 to verify identity.
    pub fn sign(&mut self, creds: &AwsCredentials, payload: &[u8]) {
        self.complement();
        let date = Utc::now();
        self.remove_header("x-amz-date");
        self.add_header("x-amz-date", &date.format("%Y%m%dT%H%M%SZ").to_string());

        // get the key x-amz-content-sha256 corresponding value.
        let (digest, _) = digest_payload(payload);

        self.remove_header("X-Amz-Decoded-Content-Length");
        self.add_header("X-Amz-Decoded-Content-Length", &format!("{}", payload.len()));

        self.remove_header("x-amz-content-sha256");
        self.add_header("x-amz-content-sha256", &digest);

        // add md5 header
        let request_md5 = Md5::digest(payload);
        self.add_header("Content-Md5", &base64::encode(&*request_md5));

        // add host header
        self.remove_header("host");
        self.add_header("host", &self.hostname.to_string());

        // organization signed_headers
        let signed_headers = signed_headers(&self.headers);

        // organization canonical_headers
        let canonical_headers = canonical_headers(&self.headers);

        let canonical_uri = self.canonical_uri.clone();

        let canonical_request = format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            &self.method,
            canonical_uri,
            self.canonical_query_string,
            canonical_headers,
            signed_headers,
            digest
        );

        // use the hashed canonical request to build the string to sign
        let hashed_canonical_request = to_hexdigest(&canonical_request);
        let scope = format!(
            "{}/{}/{}/aws4_request",
            date.format("%Y%m%d"),
            &self.region_for_service(),
            &self.service
        );
        let string_to_sign = string_to_sign(date, &hashed_canonical_request, &scope);

         // sign the string
         let signature = sign_string(
            &string_to_sign,
            creds.aws_secret_access_key(),
            date.date().naive_utc(),
            &self.region_for_service(),
            &self.service,
        );

        // build the actual auth header
        let auth_header = format!(
            "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
            &creds.aws_access_key_id(),
            scope,
            signed_headers,
            signature
        );
        self.remove_header("authorization");
        self.add_header("authorization", &auth_header);
    }
}