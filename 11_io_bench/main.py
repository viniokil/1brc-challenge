import concurrent.futures
import os

# file_path = 'test.txt'
# file_path = '../measurements_10k.txt'

# file_path = '../measurements.txt'

# def read_file_in_chunks(file_path, chunk_size=1024):
#     """Read a binary file in chunks of a specified size."""
#     with open(file_path, 'rb') as file:
#         while True:
#             chunk = file.read(chunk_size)
#             if not chunk:
#                 break  # End of file
#             yield chunk

# line_counter = 0
# for chunk in read_file_in_chunks(file_path, chunk_size=1024*16):
#     line_counter += chunk.count(b'\n')  # Handle your chunk (e.g., processing or saving it)

# print(line_counter)

file_path = '../measurements.txt'

def read_file_in_chunks(file_path, chunk_size=1024*1024):
    """Read a binary file in chunks of a specified size."""
    with open(file_path, 'rb') as file:
        while True:
            chunk_start = file.tell()
            chunk = file.read(chunk_size)
            if not chunk:
                break  # End of file

            # Ensure the chunk ends at a newline unless it's the last chunk
            if b'\n' in chunk[-1:]:
                remainder = file.readline()
                chunk += remainder

            yield chunk_start, chunk

def count_lines(data):
    """Count the '\n' characters in a given chunk of data."""
    chunk_start, chunk = data
    return chunk.count(b'\n')

def get_optimal_chunk_size(file_size, num_cores):
    """Determine an optimal chunk size based on file size and number of CPU cores."""
    base_chunk_size = 1024 * 1024  # Start with a base size of 1 MB per chunk
    if file_size < num_cores * base_chunk_size:
        # If the file is smaller than the total base chunk capacity, reduce the chunk size
        return max(file_size // num_cores, 1)
    else:
        # Otherwise, use the base chunk size or larger to minimize the number of chunks
        return min(max(file_size // (num_cores * 2), base_chunk_size), file_size // num_cores)

def main(file_path):
    num_cores = os.cpu_count() or 4  # Default to 4 if cpu_count() is None
    file_size = os.path.getsize(file_path)

    chunk_size = get_optimal_chunk_size(file_size, num_cores)

    print(f"Using chunk size: {chunk_size} bytes")

    line_counter = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_cores) as executor:
        # Map the count_lines function across the file chunks
        results = executor.map(count_lines, read_file_in_chunks(file_path, chunk_size))

        # Sum up the results
        line_counter = sum(results)

    print(line_counter)

main(file_path)
