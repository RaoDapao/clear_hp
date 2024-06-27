import os
import json
import logging
from lxml import html
from lxml.html import HtmlComment
from multiprocessing import Pool, cpu_count, Manager
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

COMMON_FOOTERS_XPATH = [
    "//footer",
    "//div[contains(@class, 'footer')]",
    "//div[contains(@id, 'footer')]",
    "//div[contains(@id, 'bottom')]",
    "//div[contains(@class, 'copyright')]",
    "//div[contains(@id, 'copyright')]",
    "//div[contains(@id, 'legal')]",
    "//div[contains(@id, 'legal')]"
]

def parse_html(content):
    """Parse the HTML content and return the root element."""
    return html.fromstring(content) if content else None

def is_empty_element(element):
    """Check if an element is empty (no text content, no child elements, and no attributes) and is not a comment."""
    if isinstance(element, HtmlComment):
        return True
    return not (element.text_content().strip() or len(element) or element.attrib)

def remove_empty_elements(soup):
    """Remove elements that have no content, attributes, or child elements."""
    if soup is not None:
        for element in reversed(list(soup.iter())):
            if is_empty_element(element):
                parent = element.getparent()
                if parent is not None:
                    parent.remove(element)

def remove_common_footers(soup):
    """Remove common footer elements."""
    deleted_content = []
    if soup is not None:
        for footer_xpath in COMMON_FOOTERS_XPATH:
            for element in soup.xpath(footer_xpath):
                parent = element.getparent()
                if parent is not None:
                    deleted_content.append(html.tostring(element, encoding='unicode', method='html'))
                    parent.remove(element)
    return deleted_content

def remove_similar_elements(parent_soup, child_soup):
    """Remove elements from child_soup that have similar content as in parent_soup."""
    deleted_content = []
    if parent_soup is not None and child_soup is not None:
        parent_text_dict = {parent_element.text_content().strip(): parent_element for parent_element in parent_soup.xpath('//*') if parent_element.text_content().strip()}

        for child_element in child_soup.xpath('//*'):
            child_text = child_element.text_content().strip()
            if child_text in parent_text_dict:
                parent = child_element.getparent()
                if parent is not None:
                    deleted_content.append(html.tostring(child_element, encoding='unicode', method='html'))
                    parent.remove(child_element)

        remove_empty_elements(child_soup)
    return deleted_content

def compare_endings(parent_soup, child_soup):
    """Compare the endings of the parent and child content and truncate the child content if necessary."""
    deleted_content = []
    if parent_soup is not None and child_soup is not None:
        parent_text = parent_soup.text_content().strip() if parent_soup.text_content() else ""
        child_text = child_soup.text_content().strip() if child_soup.text_content() else ""

        if parent_text.endswith(child_text):
            elements = child_soup.xpath(f'//*[contains(text(), "{child_text}")]')
            if elements:
                last_element = elements[-1]
                parent = last_element.getparent()
                if parent is not None:
                    deleted_content.append(html.tostring(last_element, encoding='unicode', method='html'))
                    parent.remove(last_element)

    return deleted_content

def process_parent_company(company, child_data):
    """Process each parent company's data by comparing with a random child, removing similar elements and footers."""
    logging.info(f"Processing parent company: {company.get('url')}")
    url_html_content = company.get('body_html')
    parent_soup = parse_html(url_html_content)

    deleted_content = []

    # 从子页面中选择一个进行对比
    potential_children = [child for child in child_data if child.get('p_url')]
    random_child_soup = None
    if potential_children:
        random_child = potential_children[0]
        random_child_soup = parse_html(random_child.get('body_html'))

    if parent_soup is not None:
        deleted_content.extend(remove_common_footers(parent_soup))
        if random_child_soup is not None:
            deleted_content.extend(compare_endings(parent_soup, random_child_soup))
            deleted_content.extend(remove_similar_elements(random_child_soup, parent_soup))

        remove_empty_elements(parent_soup)
        company['body_html_new'] = html.tostring(parent_soup, encoding='unicode', method='html')
        company['body_new'] = parent_soup.text_content().strip() if parent_soup.text_content() else ''
        company['body_html_deleted'] = ''.join(deleted_content)
        company['body_deleted'] = ''.join([html.fromstring(content).text_content().strip() for content in deleted_content])
    else:
        company['body_html_new'] = ''
        company['body_new'] = ''
        company['body_html_deleted'] = ''
        company['body_deleted'] = ''

    return company

def process_child_company(company, parent_data):
    """Process child company data by removing similar elements and comparing endings."""
    logging.info(f"Processing child company: {company.get('url')}")
    url_html_content = company.get('body_html')

    # 从父页面中选择一个 p_url 为空字符串的页面进行对比
    potential_parents = [parent for parent in parent_data if parent.get('p_url') == '']
    parent_soup = None
    if potential_parents:
        random_parent = potential_parents[0]
        parent_soup = parse_html(random_parent.get('body_html'))

    child_soup = parse_html(url_html_content)

    if child_soup is not None:
        remove_common_footers(child_soup)
        if parent_soup is not None:
            remove_similar_elements(parent_soup, child_soup)
            compare_endings(parent_soup, child_soup)

        remove_empty_elements(child_soup)
        company['body_html_new'] = html.tostring(child_soup, encoding='unicode', method='html')
        company['body_new'] = child_soup.text_content().strip() if child_soup.text_content() else ''
        # 不记录子页面的删除内容
        company.pop('body_html_deleted', None)
        company.pop('body_deleted', None)
    else:
        company['body_html_new'] = ''
        company['body_new'] = ''

    return company

def clean_empty_elements_in_body_html_new(company):
    """Clean empty elements in the body_html_new field of a company."""
    body_html_new = company.get('body_html_new', '')
    if body_html_new:
        soup = parse_html(body_html_new)
        remove_empty_elements(soup)
        company['body_html_new'] = html.tostring(soup, encoding='unicode', method='html') if soup is not None else ''
    return company

def process_file(src_file, dst_folder, stats):
    """Process each file and save the processed data."""
    start_time = time.time()
    logging.info(f"Processing file: {src_file}")
    with open(src_file, 'r', encoding='utf-8') as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError as e:
            logging.error(f"Failed to decode JSON file {src_file}: {e}")
            return

    updated_data = {"data": []}
    parent_data = []
    processed_urls = set()
    has_body_html = any(company.get('body_html') for company in data.get('data', []))

    if not has_body_html:
        logging.warning(f"Skipping file {src_file} due to missing body_html")
        return

    child_data = [company for company in data.get('data', []) if company.get('p_url')]

    # 处理主页面
    for company in data.get('data', []):
        if company.get('p_url') == '':  # p_url为空字符串的页面作为主页面
            parent_data.append(company)
            processed_company = process_parent_company(company, child_data)
            updated_data["data"].append(processed_company)
            processed_urls.add(company.get('url'))

    # 处理其他页面
    for company in child_data:
        processed_company = process_child_company(company, parent_data)
        updated_data["data"].append(processed_company)
        processed_urls.add(company.get('url'))

    # 清理页面的 body_html_new 中内容为空的标签
    for company in updated_data["data"]:
        clean_empty_elements_in_body_html_new(company)

    dst_file = os.path.join(dst_folder, os.path.basename(src_file))
    with open(dst_file, 'w', encoding='utf-8') as f:
        json.dump(updated_data, f, ensure_ascii=False, indent=4)
    logging.info(f"Processed and saved: {dst_file}")

    # 记录处理时间和处理的文件数
    end_time = time.time()
    stats['total_time'] += (end_time - start_time)
    stats['total_files'] += 1

def process_json_files_in_folder(src_folder, dst_folder, max_processes=None):
    """Process all JSON files in a folder using multiple processes."""
    if not os.path.exists(dst_folder):
        os.makedirs(dst_folder)

    files = [os.path.join(src_folder, filename) for filename in os.listdir(src_folder) if filename.endswith('_hp.json')]

    if max_processes is None:
        max_processes = min(len(files), cpu_count())

    manager = Manager()
    stats = manager.dict({'total_time': 0, 'total_files': 0})

    start_time = time.time()

    with Pool(processes=max_processes) as pool:
        pool.starmap(process_file, [(src_file, dst_folder, stats) for src_file in files])

    total_time = stats['total_time']
    total_files = stats['total_files']
    avg_time_per_file = total_time / total_files if total_files > 0 else 0

    end_time = time.time()
    total_elapsed_time = end_time - start_time

    logging.info(f"Total processing time (including waiting): {total_elapsed_time:.2f} seconds")
    logging.info(f"Total processing time (actual): {total_time:.2f} seconds")
    logging.info(f"Number of files processed: {total_files}")
    logging.info(f"Average time per file: {avg_time_per_file:.2f} seconds")

if __name__ == '__main__':
    src_folder = 'test'  # 请将此处替换为包含JSON文件的源文件夹路径
    dst_folder = 'test_output2'    # 请将此处替换为目标文件夹路径
    max_processes = None  # 可选：设置为None时，使用全部文件数目，否则设置为你想要的最大进程数量

    process_json_files_in_folder(src_folder, dst_folder, max_processes)
