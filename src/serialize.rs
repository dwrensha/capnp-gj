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

/// Returns None on EOF.
pub fn try_read_message<S>(
    stream: S,
    options: message::ReaderOptions) -> Promise<(S, Option<message::Reader<OwnedSegments>>), ::capnp::Error>
    where S: AsyncRead
{
    try_read_segment_table(stream).then(move |(s, r)| {
        match r {
            Some((total_words, segment_slices)) =>
                Ok(read_segments(s, total_words, segment_slices, options).map(|(s,m)| Ok((s, Some(m))))),
            None => Ok(Promise::fulfilled((s, None)))
        }
    })
}

pub fn read_message<S>(stream: S,
                       options: message::ReaderOptions) -> Promise<(S, message::Reader<OwnedSegments>),
                                                                   ::capnp::Error>
    where S: AsyncRead
{
    try_read_message(stream, options).map(|(s, r)| {
        match r {
            Some(m) => Ok((s, m)),
            None => Err(::capnp::Error::Io(::std::io::Error::new(::std::io::ErrorKind::Other,
                                                                 "premature EOF"))),
        }
    })
}

fn try_read_segment_table<S>(stream: S)
                         -> Promise<(S, Option<(usize, Vec<(usize, usize)>)>), ::capnp::Error>
    where S: AsyncRead
{
    let buf: Vec<u8> = vec![0; 8];
    stream.try_read(buf, 8).then_else(move |r| match r {
        Err(e) => Err(e.error.into()),
        Ok((stream, _, 0)) => Ok(Promise::fulfilled((stream, None))),
        Ok((_, _, n)) if n < 8 =>
            Err(::capnp::Error::Io(::std::io::Error::new(::std::io::ErrorKind::Other,
                                                         "premature EOF"))),
        Ok((stream, buf, _)) => {
            let segment_count = LittleEndian::read_u32(&buf[0..4]).wrapping_add(1) as usize;
            if segment_count >= 512 {
                unimplemented!()
                // TODO error
                //return Err()
            } else if segment_count == 0 {
                unimplemented!()
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
                                LittleEndian::read_u32(&buf[(idx * 4)..((idx + 1) * 4)]) as usize;
                            segment_slices.push((total_words, total_words + segment_len));
                            total_words += segment_len;
                        }
                        Ok((stream, Some((total_words, segment_slices))))
                    }
                }))
            } else {
                Ok(Promise::fulfilled((stream, Some((total_words, segment_slices)))))
            }
        }
    })
}

struct WordVec(Vec<Word>);

impl AsRef<[u8]> for WordVec {
    fn as_ref<'a>(&'a self) -> &'a [u8] {
        Word::words_to_bytes(&self.0[..])
    }
}

impl AsMut<[u8]> for WordVec {
    fn as_mut<'a>(&'a mut self) -> &'a mut [u8] {
        Word::words_to_bytes_mut (&mut self.0[..])
    }
}

fn read_segments<S>(stream: S,
                    total_words: usize,
                    segment_slices: Vec<(usize, usize)>,
                    options: message::ReaderOptions) -> Promise<(S, message::Reader<OwnedSegments>),
                                                                   ::capnp::Error>
    where S: AsyncRead
{
    let owned_space = WordVec(Word::allocate_zeroed_vec(total_words));
    let len = owned_space.as_ref().len();
    stream.read(owned_space, len).map_else(move |r| match r {
        Err(e) => Err(e.error.into()),
        Ok((stream, vec, _)) => {
            let segments = OwnedSegments { segment_slices: segment_slices, owned_space: vec.0 };
            Ok((stream, message::Reader::new(segments, options)))
        }
    })
}


pub struct OutputSegmentsContainer<A> where A: message::Allocator {
    message: message::Builder<A>,
    segments: ::capnp::OutputSegments<'static>
}

impl <A> OutputSegmentsContainer<A> where A: message::Allocator {
    fn new(message: message::Builder<A>) -> OutputSegmentsContainer<A> {
        let segments: ::capnp::OutputSegments<'static> = {
            let output_segments = message.get_segments_for_output();
            unsafe {
                ::std::mem::transmute(output_segments)
            }
        };
        OutputSegmentsContainer {
            message: message,
            segments: segments
        }
    }
    fn get<'a>(&'a self) -> &'a ::capnp::OutputSegments<'a> {
        &self.segments
    }
}

pub fn write_message<S, A>(stream: S,
                           message: message::Builder<A>)
                           -> Promise<(S, message::Builder<A>), ::capnp::Error>
    where S: AsyncWrite, A: message::Allocator + 'static
{
    let segments = OutputSegmentsContainer::new(message);
    write_segment_table(stream, segments).then(|(stream, segments)| {
        Ok(write_segments(stream, segments))
    }).map(|(stream, segments)| {
        Ok((stream, segments.message))
    })
}

fn write_segment_table<S, A>(stream: S,
                             segments: OutputSegmentsContainer<A>)
                             -> Promise<(S, OutputSegmentsContainer<A>), ::capnp::Error>
    where S: AsyncWrite, A: message::Allocator + 'static
{
    let segment_count = segments.get().len();
    let mut buf: Vec<u8> = vec![0; ((2 + segment_count) & !1 ) * 4];

    LittleEndian::write_u32(&mut buf[0..4], segment_count as u32 - 1);
    for idx in 0..segment_count {
        LittleEndian::write_u32(&mut buf[((idx + 1) * 4)..((idx + 2) * 4)], segments.get()[idx].len() as u32);
    }
    stream.write(buf).map_else(move |r| match r {
        Err(e) => Err(e.error.into()),
        Ok((stream, _)) => Ok((stream, segments))
    })
}

struct WritingSegment<A> where A: message::Allocator + 'static {
    idx: usize,
    segments: OutputSegmentsContainer<A>
}

impl <A> AsRef<[u8]> for WritingSegment<A> where A: message::Allocator + 'static {
    fn as_ref<'a>(&'a self) -> &'a [u8] {
        Word::words_to_bytes(self.segments.get()[self.idx])
    }
}

fn write_segments<S, A>(stream: S,
                        segments: OutputSegmentsContainer<A>)
                        -> Promise<(S, OutputSegmentsContainer<A>), ::capnp::Error>
    where S: AsyncWrite, A: message::Allocator + 'static
{
    write_segments_loop(stream, segments, 0)
}

fn write_segments_loop<S, A>(stream: S,
                             segments: OutputSegmentsContainer<A>,
                             idx: usize)
                        -> Promise<(S, OutputSegmentsContainer<A>), ::capnp::Error>
    where S: AsyncWrite, A: message::Allocator + 'static
{
    if idx >= segments.get().len() {
        Promise::fulfilled((stream, segments))
    } else {
        let buf = WritingSegment { idx: idx, segments: segments };
        stream.write(buf).then_else(move |r| match r {
            Err(e) => Err(e.error.into()),
            Ok((stream, buf)) =>
                Ok(write_segments_loop(stream, buf.segments, idx + 1))
        })
    }
}
