use prost::Message;

use crate::kv::*;

pub const PUT_REQUEST: u8 = 1;
pub const PUT_RESPONSE: u8 = 2;
pub const GET_REQUEST: u8 = 3;
pub const GET_RESPONSE: u8 = 4;

pub enum Request {
    Get(TGetRequest),
    Put(TPutRequest),
}

#[derive(Debug, thiserror::Error)]
pub enum ParseRequestError {
    #[error(transparent)]
    ProtoParseError(#[from] prost::DecodeError),
    #[error("Request type {0} is incorrect")]
    RequestTypeError(u8),
}

pub fn parse_request(
    request_type: u8,
    request_data: Vec<u8>,
) -> Result<Request, ParseRequestError> {
    match request_type {
        GET_REQUEST => Ok(Request::Get(TGetRequest::decode(request_data.as_slice())?)),
        PUT_REQUEST => Ok(Request::Put(TPutRequest::decode(request_data.as_slice())?)),
        request_type => Err(ParseRequestError::RequestTypeError(request_type)),
    }
}
