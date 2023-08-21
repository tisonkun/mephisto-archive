// Copyright 2023 tison <wander4096@gmail.com>
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

use std::{
    io,
    io::{Read, Write},
    slice,
};

use bytes::{Buf, BufMut, BytesMut};

fn interrupted(err: &io::Error) -> bool {
    matches!(err.kind(), io::ErrorKind::Interrupted)
}

fn would_block(err: &io::Error) -> bool {
    matches!(err.kind(), io::ErrorKind::WouldBlock)
}

#[derive(Debug)]
pub struct ReadContext<'a> {
    buf: &'a mut BytesMut,
    closed: bool,
}

impl<'a> ReadContext<'a> {
    pub fn buffer(&mut self) -> &mut impl Buf {
        self.buf
    }

    pub fn is_closed(&self) -> bool {
        self.closed
    }
}

#[derive(Debug)]
pub struct WriteContext<'a> {
    buf: &'a mut BytesMut,
}

impl<'a> WriteContext<'a> {
    pub fn buffer(&mut self) -> &mut BytesMut {
        self.buf
    }
}

#[derive(Debug, Default)]
pub struct Transport {
    read_buf: BytesMut,
    write_buf: BytesMut,
    written: usize,
}

impl Transport {
    pub fn write_context(&mut self) -> WriteContext {
        WriteContext {
            buf: &mut self.write_buf,
        }
    }

    pub fn read_bytes<R: Read>(&mut self, socket: &mut R) -> io::Result<ReadContext> {
        const READ_BYTES_LEN: usize = 4096;

        let mut connection_closed = false;
        loop {
            self.read_buf.reserve(READ_BYTES_LEN);
            let buf = unsafe {
                let chunk_mut_ptr = self.read_buf.chunk_mut().as_mut_ptr();
                slice::from_raw_parts_mut(chunk_mut_ptr, READ_BYTES_LEN)
            };
            match socket.read(buf) {
                Ok(0) => {
                    // Reading 0 bytes means the other side has closed the
                    // connection or is done writing, then so are we.
                    connection_closed = true;
                    break;
                }
                Ok(n) => unsafe {
                    self.read_buf.advance_mut(n);
                },
                // Would block "errors" are the OS's way of saying that the
                // connection is not actually ready to perform this I/O operation.
                Err(ref err) if would_block(err) => break,
                Err(ref err) if interrupted(err) => continue,
                // Other errors we'll consider fatal.
                Err(err) => return Err(err),
            }
        }

        Ok(ReadContext {
            buf: &mut self.read_buf,
            closed: connection_closed,
        })
    }

    pub fn write_bytes<W: Write>(&mut self, socket: &mut W) -> io::Result<()> {
        loop {
            if self.write_buf.is_empty() {
                return Ok(());
            }

            match socket.write(&self.write_buf[self.written..]) {
                Ok(n) => {
                    // consume
                    self.written += n;
                    if self.written >= self.write_buf.len() {
                        self.write_buf.clear();
                        self.written = 0;
                    }
                }
                Err(ref err) if would_block(err) => return Ok(()),
                Err(ref err) if interrupted(err) => continue,
                Err(err) => return Err(err),
            }
        }
    }
}
