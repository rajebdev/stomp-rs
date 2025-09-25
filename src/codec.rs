use crate::header::{Header, HeaderList};
use crate::frame::{Frame, Transmission, Command};
use bytes::{BytesMut, Buf};
use tokio_util::codec::{Encoder, Decoder};
use nom::{
    IResult,
    branch::alt,
    bytes::complete::{tag, is_not, take, take_while},
    character::complete::{anychar, line_ending},
    combinator::{map},
    multi::{many0, many1},
    sequence::tuple,
};

fn parse_server_command(input: &[u8]) -> IResult<&[u8], Command> {
    alt((
        map(tag("CONNECTED"), |_| Command::Connected),
        map(tag("MESSAGE"), |_| Command::Message),
        map(tag("RECEIPT"), |_| Command::Receipt),
        map(tag("ERROR"), |_| Command::Error),
    ))(input)
}

fn parse_header_character(input: &[u8]) -> IResult<&[u8], char> {
    alt((
        map(tag("\\n"), |_| '\n'),
        map(tag("\\r"), |_| '\r'),
        map(tag("\\c"), |_| ':'),
        map(tag("\\\\"), |_| '\\'),
        anychar,
    ))(input)
}

fn parse_header(input: &[u8]) -> IResult<&[u8], Header> {
    let (input, (k_bytes, _, v_bytes, _)) = tuple((
        is_not(":\r\n"),
        tag(":"),
        take_while(|b| b != b'\r' && b != b'\n'), // Allow empty values
        line_ending,
    ))(input)?;
    
    let (_, k_chars) = many1(parse_header_character)(k_bytes)?;
    
    // Handle empty header values
    let v_chars = if v_bytes.is_empty() {
        Vec::new()
    } else {
        let (_, chars) = many1(parse_header_character)(v_bytes)?;
        chars
    };
    
    let k = k_chars.into_iter().collect::<String>();
    let v = v_chars.into_iter().collect::<String>();
    
    Ok((input, Header::new_raw(k, v)))
}
fn get_body<'a>(bytes: &'a [u8], headers: &[Header]) -> IResult<&'a [u8], &'a [u8]> {
    let mut content_length = None;
    for header in headers {
        if header.0 == "content-length" {
            log::trace!("found content-length header");
            match header.1.parse::<u32>() {
                Ok(value) => content_length = Some(value),
                Err(error) => log::warn!("failed to parse content-length header: {}", error)
            }
        }
    }
    if let Some(content_length) = content_length {
        log::trace!("using content-length header: {}", content_length);
        take(content_length)(bytes)
    } else {
        log::trace!("using many0 method to parse body");
        let (input, body_parts) = many0(is_not("\0"))(bytes)?;
        let body = if body_parts.is_empty() {
            &[][..]
        } else {
            body_parts[0]
        };
        Ok((input, body))
    }
}
fn parse_frame(input: &[u8]) -> IResult<&[u8], Frame> {
    let (input, cmd) = parse_server_command(input)?;
    let (input, _) = line_ending(input)?;
    let (input, headers) = many0(parse_header)(input)?;
    let (input, _) = line_ending(input)?;
    let (input, body) = get_body(input, &headers)?;
    let (input, _) = tag("\0")(input)?;
    
    let frame = Frame {
        command: cmd,
        headers: HeaderList { headers },
        body: body.into(),
    };
    
    Ok((input, frame))
}

fn parse_transmission(input: &[u8]) -> IResult<&[u8], Transmission> {
    alt((
        map(many1(line_ending), |_| Transmission::HeartBeat),
        map(parse_frame, |f| Transmission::CompleteFrame(f)),
    ))(input)
}
pub struct Codec;

impl Encoder<Transmission> for Codec {
    type Error = std::io::Error;
    
    fn encode(&mut self, item: Transmission, buffer: &mut BytesMut) -> Result<(), Self::Error> {
        item.write(buffer);
        Ok(())
    }
}
impl Decoder for Codec {
    type Item = Transmission;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        use std::io::{Error, ErrorKind};
        use nom::Err;

        // Return None for empty buffers - nothing to decode yet
        if src.is_empty() {
            log::trace!("[CODEC] Empty buffer, waiting for more data");
            return Ok(None);
        }

        // Enhanced logging for debugging
        let hex_dump = src.iter()
            .map(|b| format!("{:02x}", b))
            .collect::<Vec<_>>()
            .join(" ");
        let ascii_dump = src.iter()
            .map(|&b| if b >= 32 && b <= 126 { b as char } else { '.' })
            .collect::<String>();
        log::debug!("[CODEC] Decoding {} bytes: {}", src.len(), hex_dump);
        log::debug!("[CODEC] ASCII: '{}'", ascii_dump);
        
        match parse_transmission(src) {
            Ok((remaining, data)) => {
                let consumed = src.len() - remaining.len();
                src.advance(consumed);
                Ok(Some(data))
            },
            Err(Err::Incomplete(_)) => {
                // Need more data
                log::trace!("[CODEC] Incomplete frame, waiting for more data");
                Ok(None)
            },
            Err(e) => {
                let error_context = if src.len() > 0 {
                    let hex_dump = src.iter().take(32)
                        .map(|b| format!("{:02x}", b))
                        .collect::<Vec<_>>()
                        .join(" ");
                    format!("on {} bytes: {}", src.len(), hex_dump)
                } else {
                    "on EMPTY buffer".to_string()
                };
                
                // Check if this might be an incomplete frame (common parsing errors)
                match e {
                    Err::Error(nom::error::Error { code: nom::error::ErrorKind::CrLf, .. }) |
                    Err::Error(nom::error::Error { code: nom::error::ErrorKind::Tag, .. }) => {
                        log::debug!("[CODEC] Possible incomplete frame ({}), waiting for more data: {}", 
                                   format!("{:?}", e), error_context);
                        Ok(None)
                    },
                    _ => {
                        log::error!("[CODEC] Parse error {:?} {}", e, error_context);
                        Err(Error::new(ErrorKind::Other, format!("parse error: {:?}", e)))
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_empty_header_values() {
        let header_with_empty_value = b"empty-header:\n";
        
        match parse_header(header_with_empty_value) {
            Ok((_remaining, header)) => {
                assert_eq!(header.0, "empty-header");
                assert_eq!(header.1, "");
                println!("Successfully parsed empty header: {:?}", header);
            }
            Err(e) => {
                panic!("Failed to parse empty header: {:?}", e);
            }
        }
    }
}
