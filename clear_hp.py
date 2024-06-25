import os
import json
import logging
from lxml import html
from lxml.html import HtmlComment
from multiprocessing import Pool, cpu_count
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

COMMON_FOOTERS_XPATH = [
    "//footer",
    "//div[contains(@class, 'footer')]",
    "//div[contains(@id, 'footer')]",
    "//div[contains(@class, 'bottom')]",
    "//div[contains(@id, 'bottom')]",
    "//div[contains(@class, 'copyright')]",
    "//div[contains(@id, 'copyright')]",
    "//div[contains(@class, 'legal')]",
    "//div[contains(@id, 'legal')]"
]

def parse_html(content):
    """Parse the HTML content and return the root element."""
    return html.fromstring(content)

def is_empty_element(element):
    """Check if an element is empty (no text content and no child elements) and is not a comment."""
    if isinstance(element, HtmlComment):
        return False
    return not (element.text_content().strip() or len(element))

def remove_empty_elements(soup):
    """Remove elements that have no content and propagate this up the tree."""
    for element in reversed(list(soup.iter())):
        if is_empty_element(element):
            parent = element.getparent()
            if parent is not None:
                parent.remove(element)

def remove_common_footers(soup):
    """Remove common footer elements."""
    for footer_xpath in COMMON_FOOTERS_XPATH:
        for element in soup.xpath(footer_xpath):
            parent = element.getparent()
            if parent is not None:
                parent.remove(element)

def remove_similar_elements(parent_soup, child_soup):
    """Remove elements from child_soup that have similar content as in parent_soup."""
    parent_text_dict = {parent_element.text_content().strip(): parent_element for parent_element in parent_soup.xpath('//*') if parent_element.text_content().strip()}

    for child_element in child_soup.xpath('//*'):
        child_text = child_element.text_content().strip()
        if child_text in parent_text_dict:
            parent = child_element.getparent()
            if parent is not None:
                parent.remove(child_element)

    remove_empty_elements(child_soup)
    return child_soup

def compare_endings(parent_soup, child_soup):
    """Compare the endings of the parent and child content and truncate the child content if necessary."""
    parent_text = parent_soup.text_content().strip()
    child_text = child_soup.text_content().strip()

    parent_end = parent_text
    child_end = child_text

    if parent_end == child_end:
        # 使用xpath找到最后一个匹配的节点并删除该节点及其之后的所有节点
        elements = child_soup.xpath(f'//*[contains(text(), "{parent_end}")]')
        if elements:
            last_element = elements[-1]
            parent = last_element.getparent()
            if parent is not None:
                parent.remove(last_element)

    return child_soup

def process_company_data(company, parent_data):
    """Process each company's data by removing similar elements and comparing endings."""
    logging.info(f"Processing company: {company.get('url')}")
    p_url = company.get('p_url')
    url_html_content = company.get('body_html')

    parent_soup = parent_data.get(p_url)
    child_soup = parse_html(url_html_content) if url_html_content else None

    if child_soup:
        remove_common_footers(child_soup)
        if parent_soup:
            logging.info(f"Removing similar elements for company: {company.get('url')}")
            child_soup = remove_similar_elements(parent_soup, child_soup)
            logging.info(f"Comparing endings for company: {company.get('url')}")
            child_soup = compare_endings(parent_soup, child_soup)

        remove_empty_elements(child_soup)
        company['body_html_new'] = html.tostring(child_soup, encoding='unicode', method='html')
        company['body_new'] = child_soup.text_content().strip()
    else:
        company['body_html_new'] = ''
        company['body_new'] = ''

    return company

def process_parent_page(parent_soup):
    """Process parent page by removing common footers and empty elements."""
    remove_common_footers(parent_soup)
    remove_empty_elements(parent_soup)
    return parent_soup

def process_file(src_file, dst_folder):
    """Process each file and save the processed data."""
    logging.info(f"Processing file: {src_file}")
    with open(src_file, 'r', encoding='utf-8') as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError as e:
            logging.error(f"Failed to decode JSON file {src_file}: {e}")
            return

    updated_data = {"data": []}
    parent_data = {}
    has_body_html = any(company.get('body_html') for company in data.get('data', []))

    if not has_body_html:
        logging.warning(f"Skipping file {src_file} due to missing body_html")
        return

    for company in data.get('data', []):
        p_url = company.get('p_url')
        url_html_content = find_body_html_by_url(data, p_url)
        if url_html_content:
            parent_soup = parse_html(url_html_content)
            parent_soup = process_parent_page(parent_soup)
            parent_data[p_url] = parent_soup
            company['body_html_new'] = html.tostring(parent_soup, encoding='unicode', method='html')
            company['body_new'] = parent_soup.text_content().strip()

    for company in data.get('data', []):
        updated_data["data"].append(process_company_data(company, parent_data))

    dst_file = os.path.join(dst_folder, os.path.basename(src_file))
    with open(dst_file, 'w', encoding='utf-8') as f:
        json.dump(updated_data, f, ensure_ascii=False, indent=4)
    logging.info(f"Processed and saved: {dst_file}")

def find_body_html_by_url(data, url):
    """Find the body HTML content by URL."""
    for company in data.get('data', []):
        if company.get('url') == url:
            return company.get('body_html')
    return None

def process_json_files_in_folder(src_folder, dst_folder, max_processes=None):
    """Process all JSON files in a folder using multiple processes."""
    if not os.path.exists(dst_folder):
        os.makedirs(dst_folder)

    files = [os.path.join(src_folder, filename) for filename in os.listdir(src_folder) if filename.endswith('.json')]

    if max_processes is None:
        max_processes = min(len(files), cpu_count())

    start_time = time.time()

    with Pool(processes=max_processes) as pool:
        pool.starmap(process_file, [(src_file, dst_folder) for src_file in files])

    end_time = time.time()
    total_time = end_time - start_time
    num_files = len(files)
    avg_time_per_file = total_time / num_files if num_files > 0 else 0

    logging.info(f"Total processing time: {total_time:.2f} seconds")
    logging.info(f"Number of files processed: {num_files}")
    logging.info(f"Average time per file: {avg_time_per_file:.2f} seconds")

if __name__ == '__main__':
    src_folder = 'source_folder'  # 请将此处替换为包含JSON文件的源文件夹路径
    dst_folder = 'test_output'    # 请将此处替换为目标文件夹路径
    max_processes = None  # 可选：设置为None时，使用全部文件数目，否则设置为你想要的最大进程数量

    process_json_files_in_folder(src_folder, dst_folder, max_processes)
