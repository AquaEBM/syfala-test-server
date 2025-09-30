use core::{error::Error, iter, mem, num::NonZeroUsize, ptr::NonNull};

fn as_bytes_mut(data: &mut [mem::MaybeUninit<f32>]) -> &mut [u8] {
    // SAFETY: all bit patterns for f32 (and u8) are valid, references have same lifetime
    unsafe {
        core::slice::from_raw_parts_mut(data.as_mut_ptr().cast(), size_of::<f32>() * data.len())
    }
}

const PORT: u16 = 6910;
const CHUNK_SIZE_FRAMES: NonZeroUsize = NonZeroUsize::new(1 << 3).unwrap();
const RB_NUM_CHUNKS: NonZeroUsize = NonZeroUsize::new(1 << 8).unwrap();

// 1
const DEFAULT_NUM_PORTS: NonZeroUsize = NonZeroUsize::MIN;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct JackBufPtrMut(NonNull<f32>);

impl JackBufPtrMut {
    #[inline]
    pub const unsafe fn increment(self) -> Self {
        Self(unsafe { self.0.add(1) })
    }

    #[inline]
    pub const unsafe fn write(mut self, val: f32) {
        // We're converting to a mutable reference here, instead of using self.write(val)
        // to make it clear to the optimizer that we have exclusive access
        *unsafe { self.0.as_mut() } = val;
    }

    #[inline]
    pub const fn from_slice(ptr: &mut [f32]) -> Self {
        Self(NonNull::new(ptr.as_mut_ptr()).unwrap())
    }

    #[inline]
    pub const fn dangling() -> Self {
        Self(NonNull::dangling())
    }
}

unsafe impl Send for JackBufPtrMut {}
unsafe impl Sync for JackBufPtrMut {}

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);

    let num_ports = args
        .next()
        .as_deref()
        .map(str::parse)
        .unwrap_or(Ok(DEFAULT_NUM_PORTS))?;

    let socket = std::net::UdpSocket::bind(("0.0.0.0", PORT)).unwrap();

    let net_chunk_size_spls = CHUNK_SIZE_FRAMES.checked_mul(num_ports).unwrap();

    let rb_size_samples = net_chunk_size_spls.checked_mul(RB_NUM_CHUNKS).unwrap();

    let network_thread = std::thread::current();

    let (client, _status) =
        jack::Client::new("SERVER", jack::ClientOptions::NO_START_SERVER).unwrap();

    let mut ports = Box::from_iter((0..num_ports.get()).map(|i| {
        client
            .register_port(&format!("output{i}"), jack::AudioOut::default())
            .unwrap()
    }));

    let mut port_buf_ptrs =
        Box::from_iter(iter::repeat_with(JackBufPtrMut::dangling).take(num_ports.get()));

    // client.connect_ports_by_name("SERVER:output", "system:playback_4").unwrap();
    // client.connect_ports_by_name("SERVER:output", "system:playback_3").unwrap();

    let (mut tx, mut rx) = rtrb::RingBuffer::new(rb_size_samples.get());

    let reader_async_client = jack::contrib::ClosureProcessHandler::with_state(
        (),
        move |_, _client, scope| {
            let mut remaining_frames = scope.n_frames() as usize;

            for (port, ptr) in ports.iter_mut().zip(&mut port_buf_ptrs) {
                *ptr = JackBufPtrMut::from_slice(port.as_mut_slice(scope))
            }

            while let Some(rem) = NonZeroUsize::new(remaining_frames) {
                let frames = CHUNK_SIZE_FRAMES.min(rem);

                let Ok(read_chunk) = rx.read_chunk(num_ports.checked_mul(frames).unwrap().get())
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
                        *buf = unsafe { buf.increment() };
                    }
                }

                read_chunk.commit_all();

                remaining_frames -= frames.get();

                if rb_size_samples.get() - rx.slots() >= net_chunk_size_spls.get() {
                    network_thread.unpark();
                }
            }

            jack::Control::Continue
        },
        |_, _, _| jack::Control::Continue,
    );

    let _active_client = client.activate_async((), reader_async_client);

    loop {
        let Ok(mut write_chunk) = tx.write_chunk_uninit(net_chunk_size_spls.get()) else {
            std::thread::park();
            continue;
        };

        let (slice, _) = write_chunk.as_mut_slices();

        let slice = as_bytes_mut(slice);
        socket.recv(slice).unwrap();

        unsafe { write_chunk.commit_all() };
    }
}
