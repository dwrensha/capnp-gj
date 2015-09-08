// Copyright (c) 2015 Sandstorm Development Group, Inc. and contributors
// Licensed under the MIT License:
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

use byteorder::{ByteOrder, LittleEndian};
use capnp::{Word, message};
use gj::Promise;
use gj::io::{AsyncRead, AsyncWrite};

pub struct OwnedSegments {
    segment_slices : Vec<(usize, usize)>,
    owned_space : Vec<Word>,
}

impl message::ReaderSegments for OwnedSegments {
    fn get_segment<'a>(&'a self, id: u32) -> Option<&'a [Word]> {
        if id < self.segment_slices.len() as u32 {
            let (a, b) = self.segment_slices[id as usize];
            Some(&self.owned_space[a..b])
        } else {
            None
        }
    }
}

pub fn read_message<S>(stream: S,
                       options: message::ReaderOptions) -> Promise<(S, message::Reader<OwnedSegments>),
                                                                   ::capnp::Error>
    where S: AsyncRead
{
    read_segment_table(stream).then(move |(stream, total_words, segment_slices)| {
        Ok(read_segments(stream, total_words, segment_slices, options))
    })
}

fn read_segment_table<S>(stream: S)
                         -> Promise<(S, usize, Vec<(usize, usize)>), ::capnp::Error>
    where S: AsyncRead
{
    let buf: Vec<u8> = vec![0; 8];
    stream.read(buf, 8).then_else(move |r| match r {
        Err(e) => Err(e.error.into()),
        Ok((stream, buf, _)) => {
            let segment_count = LittleEndian::read_u32(&buf[0..4]).wrapping_add(1) as usize;
            if segment_count >= 512 {
                // TODO error
                //return Err()
            } else if segment_count == 0 {
                // TODO error. too few segments.
            }
            let mut segment_slices = Vec::with_capacity(segment_count);
            let mut total_words = LittleEndian::read_u32(&buf[4..8]) as usize;
            segment_slices.push((0, total_words));
            if segment_count > 1 {
                let buf: Vec<u8> =
                    if segment_count == 2 {
                        buf
                    } else {
                        vec![0; 4 * (segment_count & !1)]
                    };
                let buf_len = buf.len();
                Ok(stream.read(buf, buf_len).map_else(move |r| match r {
                    Err(e) => Err(e.error.into()),
                    Ok((stream, buf, _)) => {
                        for idx in 0..(segment_count - 1) {
                            let segment_len =
                                LittleEndian::read_u32(&buf[(idx * 4)..(idx * 4 + 1)]) as usize;
                            segment_slices.push((total_words, total_words + segment_len));
                            total_words += segment_len;
                        }
                        Ok((stream, total_words, segment_slices))
                    }
                }))
            } else {
                Ok(Promise::fulfilled((stream, total_words, segment_slices)))
            }
        }
    })
}



fn read_segments<S>(_stream: S,
                    total_words: usize,
                    _segment_slices: Vec<(usize, usize)>,
                    _options: message::ReaderOptions) -> Promise<(S, message::Reader<OwnedSegments>),
                                                                   ::capnp::Error>
    where S: AsyncRead
{
    let _owned_space = Word::allocate_zeroed_vec(total_words);
    unimplemented!()
}


pub fn write_message<S, A>(_stream: S,
                           _message: message::Builder<A>) -> Promise<(S, message::Builder<A>), ::capnp::Error>
    where S: AsyncWrite, A: message::Allocator + 'static
{
    unimplemented!()
}
