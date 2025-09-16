import re
import httpx

def extract_http_refs(filename):
    with open(filename, "r") as f:
        content = f.read()
    urls = re.findall(r'"\$ref":\s*"(https?://[^"]+)"', content)
    return urls, content


if __name__ == '__main__':
    urls, file_content = extract_http_refs('schema.json')

    urls = sorted(urls, key=lambda url: len(url), reverse=True)

    for url in urls:
        response = httpx.get(url, follow_redirects=True)
        response.raise_for_status()
        filename = f"{url.split('/')[-1]}.json"
        with open(filename, 'w') as f:
           f.write(response.text)
           print(f"Downloaded {url} to {url.split('/')[-1]}")
           file_content = file_content.replace(f"\"{url}\"", f"\"{filename}\"")

    with open('schema_new.json', 'w') as f:
        f.write(file_content)
        print("Updated schema.json with local references")

