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

pub mod shutdown;
pub mod transport;

#[cfg(test)]
mod tests {
    use std::{
        io::Write,
        mem::size_of,
        net::{TcpListener, TcpStream},
    };

    use bytes::{Buf, BufMut};

    use crate::transport::Transport;

    #[test]
    fn it_works() {
        let t1 = std::thread::spawn(|| {
            let listener = TcpListener::bind("127.0.0.1:8080").unwrap();
            let (mut socket, _) = listener.accept().unwrap();
            socket.set_nonblocking(true).unwrap();
            let mut transport = Transport::default();
            loop {
                let mut read_ctx = transport.read_bytes(&mut socket).unwrap();
                if read_ctx.buffer().remaining() >= size_of::<u64>() {
                    println!("req = {}", read_ctx.buffer().get_u64());
                    break;
                }
            }
            transport.write_context().buffer().put_u64(21);
            transport.write_bytes(&mut socket).unwrap();
            socket.flush().unwrap();
            loop {
                let read_ctx = transport.read_bytes(&mut socket).unwrap();
                if read_ctx.is_closed() {
                    println!("server closed");
                    break;
                } else {
                    println!("server not closed");
                }
            }
        });

        let t2 = std::thread::spawn(|| {
            let mut socket = TcpStream::connect("127.0.0.1:8080").unwrap();
            socket.set_nonblocking(true).unwrap();
            let mut transport = Transport::default();
            transport.write_context().buffer().put_u64(42);
            transport.write_bytes(&mut socket).unwrap();
            socket.flush().unwrap();
            loop {
                let mut read_ctx = transport.read_bytes(&mut socket).unwrap();
                if read_ctx.buffer().remaining() >= size_of::<u64>() {
                    println!("res = {}", read_ctx.buffer().get_u64());
                    break;
                }
            }
        });

        t1.join().unwrap();
        t2.join().unwrap();
    }
}
