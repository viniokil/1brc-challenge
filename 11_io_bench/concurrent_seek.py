import concurrent.futures
import os

def process_chunk(file_path, start, end):
    """Read and count new lines in a specific chunk of the file."""
    with open(file_path, 'rb') as file:
        file.seek(start)
        # Read only up to the end of this chunk's boundary
        chunk = file.read(end - start)
        return chunk.count(b'\n')

def get_chunks(file_size, num_chunks):
    """Generate start and end offsets for each chunk."""
    chunk_size = file_size // num_chunks
    chunks = []
    start = 0
    for i in range(num_chunks - 1):
        end = start + chunk_size
        chunks.append((start, end))
        start = end
    # Last chunk goes to the end of the file
    chunks.append((start, file_size))
    return chunks

def adjust_chunks(file_path, chunks):
    """Adjust chunks so that they don't split lines."""
    with open(file_path, 'rb') as file:
        adjusted_chunks = []
        for start, end in chunks:
            file.seek(end)
            # Read until the end of the line, unless we're at the end of the file
            if end != os.path.getsize(file_path):
                while file.read(1) != b'\n':
                    end += 1
            adjusted_chunks.append((start, end))
        return adjusted_chunks

def main(file_path):
    num_workers = os.cpu_count() or 4  # Default to 4 if cpu_count() is None
    file_size = os.path.getsize(file_path)

    # Calculate initial chunks to be processed by each worker
    initial_chunks = get_chunks(file_size, num_workers)

    # Adjust chunks to ensure they end on newline boundaries
    chunks = adjust_chunks(file_path, initial_chunks)

    total_newlines = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        # Create a future for each chunk
        future_to_chunk = {executor.submit(process_chunk, file_path, start, end): (start, end)
                           for start, end in chunks}
        for future in concurrent.futures.as_completed(future_to_chunk):
            total_newlines += future.result()

    print(f"Total newlines: {total_newlines}")

if __name__ == '__main__':
    import cProfile
    file_path = '../measurements.txt'

    cProfile.run("main(file_path)")

