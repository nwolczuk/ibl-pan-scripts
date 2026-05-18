import math
from pathlib import Path

INPUT_DIR = "bibnau_links_input"
OUTPUT_DIR = "bibnau_links_output"
LINES_PER_FILE = 50_000

def load_links(input_path):
    input_path = Path(input_path)
    all_links = []

    print("Loading links from files...")
    for txt_file in input_path.glob("*.txt"):
        with open(txt_file, 'r', encoding='utf-8') as txt:
            for line in txt.readlines():
                line = line.strip().split(' | ')
                if line:
                    all_links.append(line[0])
    print('Links loaded.')
    return all_links

def save_links_in_chunks(links, output_dir="output_links", chunk_size=50_000):
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)

    total_files = math.ceil(len(links) / chunk_size)

    for i in range(total_files):
        start = i * chunk_size
        end = start + chunk_size
        chunk = links[start:end]

        file_path = output_path / f"links_part_{i+1}.txt"
        with open(file_path, "w", encoding="utf-8") as f:
            f.write("\n".join(chunk))

    print(f"{total_files} files saved.")


def main():
    all_links = load_links(INPUT_DIR)
    save_links_in_chunks(all_links, OUTPUT_DIR, LINES_PER_FILE)

if __name__=="__main__":
    main()