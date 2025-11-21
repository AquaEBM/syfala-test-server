use core::{cell::OnceCell, error::Error, iter, num, ptr};
use std::{collections::HashMap, io};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};

const fn as_bytes_mut(data: &mut [Sample]) -> &mut [u8] {
    // SAFETY: all bit patterns for Sample u8 are valid, references have same lifetime
    unsafe {
        core::slice::from_raw_parts_mut(
            data.as_mut_ptr().cast(),
            // shouldn't panic, but wrapping around would be incorrect
            SAMPLE_SIZE.get().checked_mul(data.len()).unwrap(),
        )
    }
}

const fn as_bytes(data: &[Sample]) -> &[u8] {
    // SAFETY: all bit patterns for u8 are valid, references have same lifetime
    unsafe {
        core::slice::from_raw_parts(
            data.as_ptr().cast(),
            // shouldn't panic, but wrapping around would be incorrect
            SAMPLE_SIZE.get().checked_mul(data.len()).unwrap()
        )
    }
}

const fn nz(x: usize) -> num::NonZeroUsize {
    num::NonZeroUsize::new(x).unwrap()
}

const DEFAULT_PORT: u16 = 6910;

type Sample = f32;
const SILENCE: Sample = 0.;

const SAMPLE_SIZE: num::NonZeroUsize = nz(size_of::<Sample>());

const SAMPLE_RATE: f64 = 48000.;

const RB_SIZE_SECONDS: f64 = 4.;

const RB_SIZE_FRAMES: num::NonZeroUsize = nz((SAMPLE_RATE * RB_SIZE_SECONDS) as usize);

const MAX_DATAGRAM_SIZE: num::NonZeroUsize = nz(1452);

const MAX_SAMPLE_DATA_SIZE_PER_DATAGRAM: num::NonZeroUsize =
    nz(MAX_DATAGRAM_SIZE.get().strict_sub(size_of::<u64>()));
const MAX_SPLS_PER_DATAGRAM: num::NonZeroUsize =
    nz(MAX_SAMPLE_DATA_SIZE_PER_DATAGRAM.get() / SAMPLE_SIZE.get());

const DEFAULT_NUM_PORTS: num::NonZeroUsize = num::NonZeroUsize::MIN; // AKA 1
const CHUNK_SIZE_FRAMES: num::NonZeroUsize = nz(1 << 4);
const EVENT_QUEUE_SIZE: num::NonZeroUsize = nz(128);

const JITTER_BUF_SIZE_SECONDS: f64 = 0.005;
const JITTER_BUF_SIZE_FRAMES: usize = (SAMPLE_RATE * JITTER_BUF_SIZE_SECONDS) as usize;

async fn read_exact_array<const N: usize>(
    reader: &mut (impl AsyncRead + Unpin + ?Sized),
) -> io::Result<[u8; N]> {
    let mut buf = [0; N];
    reader.read_exact(&mut buf).await?;
    Ok(buf)
}

enum TCPEvent {
    NewClient {
        addr: core::net::SocketAddr,
        rx: rtrb::Consumer<Sample>,
        n_ports: num::NonZeroUsize,
        tx: rtrb::Producer<Sample>,
    },

    RemoveClient {
        addr: core::net::SocketAddr,
    },
}

struct ClientTx {
    tx: rtrb::Producer<Sample>,
    sample_index: u64,
    rx_cell: Option<(rtrb::Consumer<Sample>, num::NonZeroUsize)>,
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash)]
struct JackBufPtrMut(ptr::NonNull<Sample>);

impl JackBufPtrMut {
    #[inline]
    pub const unsafe fn increment(&mut self) {
        *self = Self(unsafe { self.0.add(1) })
    }

    #[inline]
    pub const unsafe fn write(&mut self, val: Sample) {
        // We're converting to a mutable reference here, instead of using self.write(val)
        // to make it clear to the optimizer that we have exclusive access
        *unsafe { self.0.as_mut() } = val;
    }

    #[inline]
    pub const fn from_slice(ptr: &mut [Sample]) -> Self {
        Self(ptr::NonNull::new(ptr.as_mut_ptr()).unwrap())
    }

    #[inline]
    pub const fn dangling() -> Self {
        Self(ptr::NonNull::dangling())
    }
}

unsafe impl Send for JackBufPtrMut {}
unsafe impl Sync for JackBufPtrMut {}

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);

    let Some(max_num_ports) = num::NonZeroUsize::new(
        args.next()
            .as_deref()
            .map(|s| s.parse().unwrap())
            .unwrap_or(DEFAULT_NUM_PORTS.get()),
    ) else {
        return Err("Attempt to launch server with 0 inputs. Shutting down...".into());
    };

    // a ring buffer of ring buffers hahaaha
    let (mut rx_sender, mut rx_receiver) = rtrb::RingBuffer::<(
        rtrb::Consumer<Sample>,
        num::NonZeroUsize,
    )>::new(EVENT_QUEUE_SIZE.get());

    let (client, _status) = jack::Client::new("SERVER", jack::ClientOptions::NO_START_SERVER)?;

    let mut ports = Box::from_iter((1..=max_num_ports.get()).map(|i| {
        client
            .register_port(&format!("output{i}"), jack::AudioOut::default())
            .unwrap()
    }));

    let mut port_buf_ptrs =
        Box::from_iter(iter::repeat_with(JackBufPtrMut::dangling).take(max_num_ports.get()));

    // Man, you were so close, type inference. So. Freaking. Close.
    let mut rxs = Vec::<((rtrb::Consumer<Sample>, _), _)>::with_capacity(max_num_ports.get());

    // Thread 1: JACK Client
    let reader_async_client = jack::contrib::ClosureProcessHandler::new(move |_client, scope| {

        let Some(frames) = num::NonZeroUsize::new(scope.n_frames() as usize) else {
            return jack::Control::Continue;
        };

        for (port, ptr) in ports.iter_mut().zip(&mut port_buf_ptrs) {
            *ptr = JackBufPtrMut::from_slice(port.as_mut_slice(scope))
        }

        rxs.retain(|((rx, _), _)| !rx.is_abandoned());

        while let Ok(rx) = rx_receiver.pop() {
            rxs.push((rx, OnceCell::new()));
        }

        let mut port_buf_ptrs = port_buf_ptrs.as_mut();

        let last_frame_time = scope.last_frame_time();

        for ((rx, n_ports), frame_counter) in &mut rxs {
            let bufs = &mut port_buf_ptrs[..n_ports.get()];

            let _ = frame_counter.get_or_init(|| last_frame_time);
            let frame_counter = frame_counter.get_mut().unwrap();

            // Dubious behavior in some cases where if last_frame_time is less than frame counter
            // Does that ever happen?
            let n_missed_frames = last_frame_time.wrapping_sub(*frame_counter) as usize;

            let n_requested_frames = frames.checked_add(n_missed_frames).unwrap();
            let n_available_frames = rx.slots() / *n_ports;

            let n_read_frames = n_requested_frames.get().min(n_available_frames);

            let mut ptrs_iter = bufs.iter_mut();

            // We need a Deinterleaver struct like on the client side
            for sample in rx
                .read_chunk(n_read_frames.checked_mul(n_ports.get()).unwrap())
                .unwrap()
                .into_iter()
                // JACK zero-fills outputs so just skip them
                .skip(n_missed_frames.checked_mul(n_ports.get()).unwrap())
            {
                // deinterleave chunk contents

                // we cannot use Iterator::cycle or something like iter::repeat + flatten
                // so we have to resort to doing whatever this is
                let ptr = if let Some(ptr) = ptrs_iter.next() {
                    ptr
                } else {
                    ptrs_iter = bufs.iter_mut();
                    // bufs has non-zero length, so this, at least, always succeeds
                    ptrs_iter.next().unwrap()
                };

                unsafe { ptr.write(sample) };

                // SAFETY: this happens at most `frames` times for this pointer,
                // guaranteeing this stays within the buffer
                unsafe { ptr.increment() };
            }

            *frame_counter = frame_counter.wrapping_add(n_read_frames as u32);

            port_buf_ptrs = &mut port_buf_ptrs[n_ports.get()..];
        }

        jack::Control::Continue
    });

    let active_client = client.activate_async((), reader_async_client)?;

    let client = active_client.as_client();

    for i in 1..=max_num_ports.get() {
        let string = format!("SERVER:output{i}");

        client.connect_ports_by_name(&string, "system:playback_3")?;
        client.connect_ports_by_name(&string, "system:playback_4")?;
    }

    let (mut tx_sender, mut tx_receiver) = rtrb::RingBuffer::new(EVENT_QUEUE_SIZE.get());

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()?;

    std::thread::spawn(move || {
        let mut task_set =
            tokio::task::JoinSet::<(core::net::SocketAddr, num::NonZeroUsize)>::new();

        tokio::task::LocalSet::new().block_on(&rt, async move {
            let listener = tokio::net::TcpListener::bind("0.0.0.0:6910")
                .await
                .expect("ERROR: Failed to create TCP listener");

            let mut num_available_ports = max_num_ports.get();

            loop {
                let (mut stream, addr) = listener
                    .accept()
                    .await
                    .expect("ERROR: Failed to accept TCP connection");

                println!("{addr} requested to connect");

                while let Some(task_res) = task_set.try_join_next() {
                    let (addr, n_ports) = task_res.unwrap();
                    tx_sender
                        .push(TCPEvent::RemoveClient { addr })
                        .expect("ERROR: TCP event queue too contended");

                    num_available_ports += n_ports.get();
                }

                if let Err(e) = stream.set_nodelay(true) {
                    eprintln!("WARNING: Failed with to set nodelay for stream {addr}. Error {e}");
                }

                // read the requested port count

                let requested_num_ports = read_exact_array(&mut stream)
                    .await
                    .map(usize::from_be_bytes)
                    .expect("ERROR: couldn't read requested port count");

                println!("{addr} requested {requested_num_ports} ports...");

                let n_ports = if requested_num_ports > num_available_ports {
                    println!("WARNING: Got {num_available_ports} instead");
                    num_available_ports
                } else {
                    println!("Accepted!");
                    requested_num_ports
                };

                // never underflows because of the previous check
                num_available_ports -= n_ports;
                println!("{num_available_ports} available ports remaining");

                // Return how many we can actually serve
                stream
                    .write_all(&n_ports.to_be_bytes())
                    .await
                    .expect("ERROR: Couldn't send available ports");

                // Also, return the buffering suggestion
                stream
                    .write_all(&CHUNK_SIZE_FRAMES.get().to_be_bytes())
                    .await
                    .expect("ERROR: couldn't send buffering suggestion");

                let Some(n_ports) = num::NonZeroUsize::new(n_ports) else {
                    continue;
                };
                
                let rb_size_frames = RB_SIZE_FRAMES.checked_add(JITTER_BUF_SIZE_FRAMES).unwrap();

                let rb_size_spls = rb_size_frames.checked_mul(n_ports).unwrap();

                println!("Allocating ring buffer: {rb_size_spls} samples");

                let (mut tx, rx) = rtrb::RingBuffer::<Sample>::new(rb_size_spls.get());

                let jitter_buf_size_spls = n_ports.get().strict_mul(JITTER_BUF_SIZE_FRAMES);
                for _ in 0..jitter_buf_size_spls {
                    tx.push(SILENCE).expect("jitter delay smaller than the actual ring buffer size, lol");
                }

                tx_sender
                    .push(TCPEvent::NewClient {
                        addr,
                        tx,
                        rx,
                        n_ports,
                    })
                    .expect("ERROR: TCP event queue too contended");

                task_set.spawn_local(async move {
                    // At this point, until it closes, the client shouldn't be sending anything else
                    // over TCP. This implies that, either way, we only have to wait until this call
                    // returns. This match statement just picks the right logging/error messages...
                    let mut tmp_buf = [0; 4];
                    match stream.read(&mut tmp_buf).await {
                        Ok(0) => println!("{addr} disconnected..."),
                        Ok(1..) => {
                            eprintln!("ERROR: Received unexpected data (TCP) from {addr}")
                        }
                        Err(e) => eprintln!("ERROR: {e} while waiting for {addr} to close"),
                    }

                    println!("Freeing {n_ports} channels...");

                    (addr, n_ports)
                });
            }
        })
    });

    let socket = std::net::UdpSocket::bind((core::net::Ipv4Addr::UNSPECIFIED, DEFAULT_PORT))?;

    let mut client_table = HashMap::new();

    let mut buf = [SILENCE; MAX_SPLS_PER_DATAGRAM.get() + 2];

    loop {
        // Rust doesn't provide a way to pass in MSG_TRUNC, which allows finding out the full
        // size of the original datagram. Combined with MSG_PEEK (Or UdpSocket::peek_from),
        // we can find out the source and the size of the packet before copying it anywhere
        // TODO: Can we save an extra copy by using libc directly?
        //       Is it actually worth saving on that extra copy?

        let (bytes_read, source_addr) = socket.recv_from(as_bytes_mut(&mut buf))?;

        while let Ok(event) = tx_receiver.pop() {
            match event {
                TCPEvent::NewClient {
                    addr,
                    rx,
                    n_ports,
                    tx,
                } => {
                    if client_table
                        .insert(
                            addr,
                            ClientTx {
                                tx,
                                sample_index: 0u64,
                                rx_cell: Some((rx, n_ports)),
                            },
                        )
                        .is_some()
                    {
                        unreachable!("ERROR: Clients with duplicate addresses found");
                    }
                },
                TCPEvent::RemoveClient { addr } => {
                    client_table
                        .remove(&addr)
                        .expect("ERROR: Attempt to remove non-existent client");
                },
            }
        }

        if let Some(ClientTx {
            tx,
            sample_index,
            rx_cell,
        }) = client_table.get_mut(&source_addr)
        {
            // The only irrecoverable error (actually invalid packet) as per our protocol

            let Some(num_sample_bytes) = bytes_read.checked_sub(size_of::<u64>()) else {
                eprintln!("WARNING: datagram from {source_addr} missing index field");
                continue;
            };

            // This one isn't much of an issue
            if bytes_read > MAX_DATAGRAM_SIZE.get() {
                eprintln!("WARNING: Oversized ({bytes_read}B) datagram from {source_addr}");
            }

            let num_samples = num_sample_bytes / SAMPLE_SIZE;
            if let Some(_trailing_bytes) = num::NonZeroUsize::new(num_sample_bytes % SAMPLE_SIZE) {
                eprintln!("WARNING: misaligned datagram ({bytes_read}B) from {source_addr}");
            }

            let index = u64::from_be_bytes(as_bytes(&buf)[..size_of::<u64>()].try_into().unwrap());

            // We discard samples from reordered packets, we've already zero-filled their spots
            let Some(samples_missed) = index.checked_sub(*sample_index) else {
                eprintln!("WARNING: Samples from {source_addr} reordered");
                continue;
            };

            let samples_missed = samples_missed as usize;

            let all_samples = &buf[2..][..num_samples];

            // The first samples_missed samples will be filled with zeros
            let n_requested_samples = num_samples.strict_add(samples_missed);

            let n_available_sample_slots = tx.slots().min(n_requested_samples);

            let n_zero_filled_slots = samples_missed.min(n_available_sample_slots);
            let n_written_slots = n_available_sample_slots.strict_sub(n_zero_filled_slots);

            // TODO: Is queueing the samples in lost somewhere else to resend them later worth it?
            // For now we just discard them and send zeros. In any case, the ring buffer being too
            // small is something worth alerting the developer.
            let (written, lost) = all_samples.split_at(n_written_slots);

            if lost.len() > 0 {
                eprintln!(
                    "WARNING: Ring buffer for {source_addr} full!: {} samples lost!",
                    lost.len()
                );
            }

            let mut write_chunk = tx.write_chunk_uninit(n_available_sample_slots).unwrap();

            let (start, end) = write_chunk.as_mut_slices();

            // Split the ring buffer slices into two parts, the first one will be filled with
            // zeros, we will write into the rest as much of this packet's data as we can

            let start_zero_filled_len = start.len().min(n_zero_filled_slots);
            let end_zero_filled_len = n_zero_filled_slots.strict_sub(start_zero_filled_len);

            let (rb_zero_filled_start, rb_spls_start) = start.split_at_mut(start_zero_filled_len);
            let (rb_zero_filled_end, rb_spls_end) = end.split_at_mut(end_zero_filled_len);

            // NIGHTLY: #[feature(maybe_uninit_fill)], use write_filled
            for s in iter::chain(rb_zero_filled_start, rb_zero_filled_end) {
                s.write(SILENCE);
            }

            let (packet_spls_start, packet_spls_end) = written.split_at(rb_spls_start.len());

            // NIGHTLY: #[feature(maybe_uninit_fill)], use write_copy_of_slice
            for (&i, o) in iter::zip(packet_spls_start, rb_spls_start) { o.write(i); }
            for (&i, o) in iter::zip(packet_spls_end, rb_spls_end) { o.write(i); }

            unsafe { write_chunk.commit_all() }

            *sample_index = sample_index.wrapping_add(n_available_sample_slots as u64);

            if let Some((rx, n_ports)) = rx_cell.take_if(|_| rx_sender.slots() >= 1) {
                rx_sender.push((rx, n_ports)).unwrap();
                eprintln!("Started listening for data from {source_addr}");
            }
        } else {
            eprintln!("WARNING: received datagram from unregistered address: {source_addr}");
        }
    }
}
