use core::{error::Error, iter, mem, num, ptr, sync::atomic::{AtomicUsize, Ordering}};
use std::{collections::HashMap, io::{self, Read, Write}};

fn as_bytes_mut(data: &mut [f32]) -> &mut [u8] {
    // SAFETY: all bit patterns for f32 (and u8) are valid, references have same lifetime
    unsafe {
        core::slice::from_raw_parts_mut(data.as_mut_ptr().cast(), size_of::<f32>() * data.len())
    }
}

const PORT: u16 = 6910;
const RB_MIN_NUM_FRAMES: num::NonZeroUsize = num::NonZeroUsize::new(1 << 10).unwrap();

// 1
const CHUNK_SIZE_FRAMES: num::NonZeroUsize = num::NonZeroUsize::new(1 << 4).unwrap();
const DEFAULT_NUM_PORTS: num::NonZeroUsize = num::NonZeroUsize::MIN;

fn read_exact_array<const N: usize>(
    reader: &mut (impl Read + Unpin + ?Sized),
) -> io::Result<[u8; N]> {
    let mut buf = [0; N];
    reader.read_exact(&mut buf).map(|_| buf)
}

struct ClientRx {
    rx: rtrb::Consumer<f32>,
    n_ports: num::NonZeroUsize,
}

struct ClientTx {
    tx: rtrb::Consumer<f32>,
    n_ports: num::NonZeroUsize,
    scratch_buffer: Box<[mem::MaybeUninit<f32>]>,
    
}

fn fetch_sub_saturating(a: &AtomicUsize, b: usize) -> usize {
    let mut old = a.load(Ordering::Relaxed);

    while let Err(v) = a.compare_exchange_weak(old, old.saturating_sub(b), Ordering::Relaxed, Ordering::Relaxed) {
        old = v;
    }

    old
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash)]
struct JackBufPtrMut(ptr::NonNull<f32>);

impl JackBufPtrMut {
    #[inline]
    pub const unsafe fn increment(&mut self) {
        *self = Self(unsafe { self.0.add(1) })
    }

    #[inline]
    pub const unsafe fn write(&mut self, val: f32) {
        // We're converting to a mutable reference here, instead of using self.write(val)
        // to make it clear to the optimizer that we have exclusive access
        *unsafe { self.0.as_mut() } = val;
    }

    #[inline]
    pub const fn from_slice(ptr: &mut [f32]) -> Self {
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

    let chunk_size_spls = CHUNK_SIZE_FRAMES.checked_mul(max_num_ports).unwrap();
    let rb_size_frames = num::NonZeroUsize::new(
        RB_MIN_NUM_FRAMES
            .get()
            .checked_next_multiple_of(CHUNK_SIZE_FRAMES.get())
            .unwrap(),
    )
    .unwrap();

    // a ring buffer of ring buffers hahaaha
    let (mut rb_tx, mut rb_rx) = rtrb::RingBuffer::new(128);

    let (client, _status) = jack::Client::new("SERVER", jack::ClientOptions::NO_START_SERVER)?;

    let mut ports = Box::from_iter((1..=max_num_ports.get()).map(|i| {
        client
            .register_port(&format!("output{i}"), jack::AudioOut::default())
            .unwrap()
    }));

    let mut port_buf_ptrs =
        Box::from_iter(iter::repeat_with(JackBufPtrMut::dangling).take(max_num_ports.get()));

    let network_thread = std::thread::current();

    let mut rxs = Vec::with_capacity(max_num_ports.get());

    let reader_async_client = jack::contrib::ClosureProcessHandler::new(move |_client, scope| {
        let mut remaining_frames = scope.n_frames() as usize;

        for (port, ptr) in ports.iter_mut().zip(&mut port_buf_ptrs) {
            *ptr = JackBufPtrMut::from_slice(port.as_mut_slice(scope))
        }

        while let Some(rem) = num::NonZeroUsize::new(remaining_frames) {
            rxs.retain(|ClientRx { rx, .. }| !rx.is_abandoned());

            while let Ok(rx) = rb_rx.pop() {
                rxs.push(rx);
            }

            let frames = CHUNK_SIZE_FRAMES.min(rem);
            remaining_frames -= frames.get();

            for ClientRx { rx, n_ports: n_channels } in &mut rxs {
                let Ok(read_chunk) = rx.read_chunk(n_channels.checked_mul(frames).unwrap().get())
                else {
                    continue;
                };

                let (start, end) = read_chunk.as_slices();

                let mut rb_chunk_iter = start.iter().chain(end.iter());

                for _i in 0..frames.get() {
                    // deinterleave chunk contents

                    for buf in &mut port_buf_ptrs {
                        let &sample = rb_chunk_iter.next().unwrap();

                        // SAFETY: buf is valid, and within the actual buffer's bounds
                        unsafe { buf.write(sample) };

                        // SAFETY: this happens at most `frames` times, guaranteeing this stays within the buffer
                        unsafe { buf.increment() };
                    }
                }

                read_chunk.commit_all();

                let rb_size_spls = n_channels.checked_mul(rb_size_frames).unwrap();

                if rb_size_spls.get() - rx.slots() >= chunk_size_spls.get() {
                    network_thread.unpark();
                }
            }
        }

        jack::Control::Continue
    });

    let active_client = client.activate_async((), reader_async_client)?;

    let client = active_client.as_client();

    for i in 0..max_num_ports.get() {
        client.connect_ports_by_name(
            &format!("SERVER:output{}", i + 1),
            &format!("system:playback_{}", i % 2 + 3),
        )?
    }

    let (mut event_tx, mut event_rx) = rtrb::RingBuffer::new(128);

    let tcp_thread = std::thread::spawn(move || {
        let listener = std::net::TcpListener::bind("[::]:6910").expect("ERROR: Failed to create TCP listener");

        loop {
            let (mut stream, address) = listener.accept().expect("ERROR: Failed to accept TCP connection");

            event_tx.push((address, stream)).expect("ERROR: Connection request ring buffer too contented");
        }
    });

    let mut rbs = HashMap::with_capacity(max_num_ports.get());
    let mut scratch_buffer = Box::from_iter(iter::repeat(0.).take(CHUNK_SIZE_FRAMES.checked_mul(max_num_ports).unwrap().get()));
    let mut num_available_ports = max_num_ports.get();
    let socket = std::net::UdpSocket::bind(("0.0.0.0", PORT))?;

    loop {

        while let Ok((addr, mut stream)) = event_rx.pop() {

            // read the requested port count
            let requested_num_ports = usize::from_be_bytes(read_exact_array(&mut stream)?);

            println!("Client at {addr} requested {requested_num_ports} ports...");

            let n_ports = requested_num_ports.min(num_available_ports);
            num_available_ports -= n_ports;

            // Return how many we can actually serve
            stream.write_all(&n_ports.to_be_bytes())?;

            println!("Client at {addr} got {n_ports} ports");

            // Also, return the buffering suggestion
            stream.write_all(&CHUNK_SIZE_FRAMES.get().to_be_bytes())?;

            let Some(n_ports) = num::NonZeroUsize::new(n_ports) else {
                continue;
            };

            let rb_size_spls = rb_size_frames.checked_mul(n_ports).unwrap();
            let chunk_size_spls = CHUNK_SIZE_FRAMES.checked_mul(n_ports).unwrap();

            let (tx, rx) = rtrb::RingBuffer::new(rb_size_spls.get());

            rbs.insert(addr, (n_ports, tx)).expect("ERROR: Clients with duplicate addresses found");

            rb_tx.push(ClientRx { rx, n_ports }).expect("ERROR: event ring buffer too contended");
        }

        let (size, source_addr) = socket.(as_bytes_mut(&mut scratch_buffer))?;

        assert!(size.is_multiple_of(size_of::<f32>()));


    }
}

// #[tokio::main]
// async fn main() -> Result<(), Box<dyn Error>> {

//     let (num_ports, addr) = accept_syfala_client_connection(max_num_ports.get())?;

//     let mut ring_bufs = Vec::with_capacity(num_ports.get());

// }
