use core::{
    cell::RefCell, cmp, error::Error, iter, num, ptr
};
use std::{collections::{HashMap, VecDeque}, io, rc::Rc};

use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
};

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

async fn read_exact_array<const N: usize>(
    reader: &mut (impl AsyncRead + Unpin + ?Sized),
) -> io::Result<[u8; N]> {
    let mut buf = [0; N];
    reader.read_exact(&mut buf).await.map(|_| buf)
}

enum TCPEvent {
    NewClient {
        addr: core::net::SocketAddr,
        rx: ClientRx,
        tx: ClientTx,
    },

    RemoveClient {
        addr: core::net::SocketAddr,
    },
}

struct ClientRx {
    rx: rtrb::Consumer<f32>,
    n_ports: num::NonZeroUsize,
}

struct ClientTx {
    tx: rtrb::Producer<f32>,
    n_ports: num::NonZeroUsize,
    scratch_buffer: VecDeque<f32>,
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

async fn handle_tcp_connection(
    mut stream: tokio::net::TcpStream,
    addr: core::net::SocketAddr,
    num_available_ports: usize,
    rb_size_frames: num::NonZeroUsize,
    tx_sender: Rc<RefCell<rtrb::Producer<TCPEvent>>>,
) -> Result<usize, io::Error> {
    // read the requested port count
    let requested_num_ports = read_exact_array(&mut stream)
        .await
        .map(usize::from_be_bytes)?;

    println!("Client at {addr} requested {requested_num_ports} ports...");

    let n_ports = cmp::min(num_available_ports, requested_num_ports);

    // Return how many we can actually serve
    stream.write_all(&n_ports.to_be_bytes()).await?;

    println!("Client at {addr} got {n_ports} ports");

    // Also, return the buffering suggestion
    stream
        .write_all(&CHUNK_SIZE_FRAMES.get().to_be_bytes())
        .await?;

    let Some(n_ports) = num::NonZeroUsize::new(n_ports) else {
        return Ok(0);
    };

    let rb_size_spls = rb_size_frames.checked_mul(n_ports).unwrap();

    let (tx, rx) = rtrb::RingBuffer::new(rb_size_spls.get());

    tx_sender.borrow_mut().push(TCPEvent::NewClient {
        addr,
        tx: ClientTx { tx, n_ports, scratch_buffer: VecDeque::new() },
        rx: ClientRx { rx, n_ports },
    })
        .expect("ERROR: Clients with duplicate addresses found");

    println!("Waiting for client at {addr} to disconnect...");

    // At this point, until it closes, the client shouldn't be sending anything else
    // over TCP. This implies that, either way, we will close the task when this call
    // returns. This match statement just picks the right logging/error messages...
    let mut tmp_buf = [0; 4];
    match stream.read(&mut tmp_buf).await {
        Ok(0) => println!("Client at {addr} disconnected..."),
        Ok(1..) => eprintln!("ERROR: Unexpected behavior from client at {addr}"),
        Err(e) => {
            eprintln!("ERROR: Encountered error {e} while waiting for client at {addr} to close")
        }
    }

    println!("Freeing {n_ports} channels...");

    Ok(n_ports.get())
}

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
    let (mut rx_sender, mut rx_receiver) = rtrb::RingBuffer::new(128);

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

    // Thread 1: JACK Client
    let reader_async_client = jack::contrib::ClosureProcessHandler::new(move |_client, scope| {
        let mut remaining_frames = scope.n_frames() as usize;

        for (port, ptr) in ports.iter_mut().zip(&mut port_buf_ptrs) {
            *ptr = JackBufPtrMut::from_slice(port.as_mut_slice(scope))
        }

        while let Some(rem) = num::NonZeroUsize::new(remaining_frames) {
            rxs.retain(|ClientRx { rx, .. }| !rx.is_abandoned());

            while let Ok(rx) = rx_receiver.pop() {
                rxs.push(rx);
            }

            let frames = CHUNK_SIZE_FRAMES.min(rem);
            remaining_frames -= frames.get();

            for ClientRx {
                rx,
                n_ports: n_channels,
            } in &mut rxs
            {
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

    let (tx_sender, mut tx_receiver) = rtrb::RingBuffer::new(128);

    // single-threaded runtime
    let rt = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()
            .expect("ERROR: failed to");

    // Thread 2: TCP Thread
    std::thread::spawn(move || {

        rt.block_on(async move {

            let mut num_available_ports = max_num_ports.get();
            let listener = tokio::net::TcpListener::bind("[::]:6910")
                .await
                .expect("ERROR: Failed to create TCP listener");
            let mut task_set = tokio::task::JoinSet::new();

            // TODO: Do something about this
            let tx_sender = Rc::new(RefCell::new(tx_sender));

            loop {
                let (stream, addr) = listener
                    .accept()
                    .await
                    .expect("ERROR: Failed to accept TCP connection");

                while let Some(join_result) = task_set.try_join_next() {
                    match join_result {
                        Ok(task_result) => match task_result {
                            Ok(n_ports) => num_available_ports -= n_ports,
                            Err(e) => eprintln!("TCP task for client at {addr} failed with error {e}"),
                        },
                        // We never cancel the tasks, so this necessarily means that
                        // handle_tcp_connection panicked, which we consider a bug.
                        Err(e) => unreachable!("TCP task with client at {addr} panicked with error: {e}"),
                    }
                }

                task_set.spawn_local(handle_tcp_connection(
                    stream,
                    addr,
                    num_available_ports,
                    rb_size_frames,
                    Rc::clone(&tx_sender),
                ));
            }
        })
    });

    let socket = std::net::UdpSocket::bind(("0.0.0.0", PORT))?;

    let mut client_table = HashMap::new();

    const MAX_SPLS_PER_DATAGRAM: usize = 368;
    const MAX_DATAGRAM_SIZE_BYTES: usize = MAX_SPLS_PER_DATAGRAM * size_of::<f32>();
    let mut buf = [0 ; MAX_DATAGRAM_SIZE_BYTES + 1];

    loop {

        while let Ok(event) = tx_receiver.pop() {

            match event {
                TCPEvent::NewClient { addr, rx, tx } => {
                    client_table.insert(addr, tx).expect("ERROR: Clients with duplicate addreses found");

                    rx_sender.push(rx).expect("ERROR: Audio thread ring buffer too contended");
                },
                TCPEvent::RemoveClient { addr } => todo!(),
            }
            
        }

        let (bytes_read, source_addr) = socket.recv_from(&mut buf)?;

        if let Some(ClientTx { tx, n_ports, scratch_buffer }) = client_table.get_mut(&source_addr) {
            let chunk_size_spls = CHUNK_SIZE_FRAMES.checked_mul(*n_ports).unwrap();
            
            assert_eq!(
                bytes_read % chunk_size_spls, 0,
                "ERROR: Incomplete datagram from {source_addr}"
            );

            if bytes_read == 0 {
                eprintln!("WARNING: Zero-sized datagram from {source_addr}");
                continue;
            }

            let n_frames = bytes_read / bytes_read;



            assert!(
                bytes_read <= MAX_DATAGRAM_SIZE_BYTES,
                "ERROR: Oversized datagram from {source_addr}"
            );

            scratch_buffer.drain(..);

        } else {
            eprintln!("WARNING: received datagram from unknown address {source_addr}");
        }
    }
}