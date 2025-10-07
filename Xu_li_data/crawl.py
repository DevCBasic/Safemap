#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Complete SafeMap Crawler - RSS → Full Article → Extract
Flow: RSS Feed → Article URLs → Fetch Full Content → Extract Info → Save JSON
Improved: Better rule-based summarization when no LLM available
FIXED: Better RSS crawling with debug info and fallback URLs
"""
from pathlib import Path
import os
import feedparser
import requests
import json
import re
import time
import random
import logging
import hashlib
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from typing import Optional, Dict, List

# Setup logging - chỉ ghi vào file, không hiện terminal
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crawler.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# Tạo logger riêng cho terminal (chỉ thông tin quan trọng)
console = logging.StreamHandler()
console.setLevel(logging.WARNING)  # Chỉ hiện WARNING và ERROR
console.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
logger.addHandler(console)

# User agents for rotation
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
]

# OpenAI setup with proper validation
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
OPENAI_AVAILABLE = False

if OPENAI_API_KEY and OPENAI_API_KEY != "":
    try:
        import openai
        # Validate API key format
        if OPENAI_API_KEY.startswith('sk-') and len(OPENAI_API_KEY) > 20:
            openai.api_key = OPENAI_API_KEY
            OPENAI_AVAILABLE = True
            logger.info("✓ OpenAI API configured and validated")
        else:
            logger.warning("⚠ Invalid OpenAI API key format - using rule-based extraction")
    except ImportError:
        logger.warning("⚠ openai package not installed - using rule-based extraction")
else:
    logger.info("ℹ OpenAI API not configured - using enhanced rule-based extraction")

# News sources - UPDATED with working RSS URLs (verified)
SOURCES = {
    "VnExpress": {
        "rss": [
            "https://vnexpress.net/rss/tin-moi-nhat.rss",
            "https://vnexpress.net/rss/thoi-su.rss",
            "https://vnexpress.net/rss/xa-hoi.rss"
        ],
        "base_url": "https://vnexpress.net"
    },
    "Dân Trí": {
        "rss": [
            "https://dantri.com.vn/rss/home.rss",
            "https://dantri.com.vn/rss/thoi-su.rss",
            "https://dantri.com.vn/rss/su-kien.rss"
        ],
        "base_url": "https://dantri.com.vn"
    },
    "Vietnamnet": {
        "rss": [
            "https://vietnamnet.vn/rss/thoi-su.rss",
            "https://vietnamnet.vn/rss/xa-hoi.rss"
        ],
        "base_url": "https://vietnamnet.vn"
    },
    "Tuổi Trẻ": {
        "rss": [
            "https://tuoitre.vn/rss/tin-moi-nhat.rss",
            "https://tuoitre.vn/rss/thoi-su.rss",
            "https://tuoitre.vn/rss/xa-hoi.rss"
        ],
        "base_url": "https://tuoitre.vn"
    },
    "Thanh Niên": {
        "rss": [
            "https://thanhnien.vn/rss/home.rss",
            "https://thanhnien.vn/rss/thoi-su.rss"
        ],
        "base_url": "https://thanhnien.vn"
    }
}

# Hanoi locations
HANOI_DISTRICTS = [
    "Ba Đình", "Hoàn Kiếm", "Hai Bà Trưng", "Đống Đa",
    "Tây Hồ", "Cầu Giấy", "Thanh Xuân", "Hoàng Mai",
    "Long Biên", "Bắc Từ Liêm", "Nam Từ Liêm", "Hà Đông"
]

HANOI_KEYWORDS = ["Hà Nội", "Ha Noi", "Thủ đô", "TP Hà Nội"]

# Keywords to filter out non-content sentences
NOISE_KEYWORDS = [
    "lưu bài", "bỏ lưu", "đồng ý", "chia sẻ", "thành công", 
    "xem lại bài viết", "tin bài đã lưu", "sự kiện", "theo dõi",
    "đăng ký nhận tin", "like", "share", "comment", "bình luận",
    "phóng viên", "nguồn:", "tin liên quan", "xem thêm",
    "cập nhật lúc", "chuyên mục", "tags:", "từ khóa"
]

def get_random_headers():
    """Get random user agent"""
    return {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
    }

def fetch_url(url: str, timeout: int = 15) -> Optional[requests.Response]:
    """Fetch URL with retry"""
    for attempt in range(3):
        try:
            response = requests.get(
                url, 
                headers=get_random_headers(),
                timeout=timeout,
                allow_redirects=True
            )
            if response.status_code == 200:
                return response
            logger.warning(f"Status {response.status_code} for {url}")
        except requests.RequestException as e:
            logger.warning(f"Attempt {attempt+1} failed for {url}: {e}")
            time.sleep(2 ** attempt)
    return None

def clean_sentence(sentence: str) -> str:
    """Clean and validate sentence"""
    sentence = sentence.strip()
    # Remove extra whitespace
    sentence = re.sub(r'\s+', ' ', sentence)
    return sentence

def is_valid_content_sentence(sentence: str) -> bool:
    """Check if sentence is valid content (not noise/UI elements)"""
    sentence_lower = sentence.lower()
    
    # Too short
    if len(sentence) < 30:
        return False
    
    # Contains noise keywords
    if any(noise in sentence_lower for noise in NOISE_KEYWORDS):
        return False
    
    # Too many special characters
    special_char_ratio = len(re.findall(r'[^\w\s]', sentence)) / len(sentence)
    if special_char_ratio > 0.3:
        return False
    
    # Contains meaningful Vietnamese words
    meaningful_words = ['là', 'có', 'được', 'tại', 'này', 'đã', 'sẽ', 'người', 'theo']
    if not any(word in sentence_lower for word in meaningful_words):
        return False
    
    return True

def extract_article_content(url: str) -> Optional[Dict]:
    """Fetch và extract nội dung đầy đủ của bài báo"""
    response = fetch_url(url)
    if not response:
        return None
    
    try:
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Extract title
        title = ""
        h1 = soup.find('h1')
        if h1:
            title = h1.get_text(strip=True)
        elif soup.title:
            title = soup.title.get_text(strip=True)
        
        # Extract main content
        content_text = ""
        
        content_selectors = [
            'article.fck_detail', 'div.fck_detail',
            'div.singular-content', 'div.dt-news__content',
            'div.ArticleContent', 'div.article-content',
            'div#main-detail-content', 'div.detail-content',
            'div.detail-content-body', 'div.cate-24h-content-detail',
            'article', 'div[class*="content"]', 'div[class*="article"]',
        ]
        
        for selector in content_selectors:
            content_div = soup.select_one(selector)
            if content_div:
                for tag in content_div(['script', 'style', 'iframe', 'noscript', 'button', 'a']):
                    tag.decompose()
                for popup_tag in content_div.find_all(
                    ["div", "section"],
                    {"class": re.compile(r"popup|modal|share|save|button|action|system|success|confirm|notification", re.I),
                     "id": re.compile(r"popup|modal|share|save|button|action|system|success|confirm|notification", re.I)}
                ):
                    popup_tag.decompose()
                
                paragraphs = content_div.find_all('p', recursive=True)
                texts = []
                for p in paragraphs:
                    text = clean_sentence(p.get_text(separator=' ', strip=True))
                    if is_valid_content_sentence(text):
                        texts.append(text)
                
                content_text = ' '.join(texts)
                if len(content_text) > 200:
                    break
        
        if len(content_text) < 200:
            paragraphs = soup.find_all('p')
            texts = [clean_sentence(p.get_text(strip=True)) 
                    for p in paragraphs 
                    if is_valid_content_sentence(p.get_text(strip=True))]
            content_text = ' '.join(texts)
        
        # Extract publish date
        publish_date = ""
        date_metas = [
            ('property', 'article:published_time'),
            ('property', 'og:updated_time'),
            ('name', 'pubdate'),
            ('name', 'date'),
            ('itemprop', 'datePublished'),
        ]
        
        for attr, value in date_metas:
            meta = soup.find('meta', {attr: value})
            if meta and meta.get('content'):
                publish_date = meta['content']
                break
        
        if not publish_date:
            time_tag = soup.find('time')
            if time_tag:
                publish_date = time_tag.get('datetime', '') or time_tag.get_text()
        
        if not content_text or len(content_text) < 100:
            logger.debug(f"Content too short for {url}")
            return None
        
        return {
            'title': title,
            'content': content_text,
            'publish_date': publish_date,
            'url': url
        }
        
    except Exception as e:
        logger.error(f"Error extracting content from {url}: {e}")
        return None

def extract_with_llm(title: str, content: str) -> Optional[Dict]:
    """Extract info using LLM"""
    if not OPENAI_AVAILABLE:
        return None

    prompt = f"""Phân tích bài báo sau và trích xuất thông tin:

Tiêu đề: {title}
Nội dung: {content[:3000]}

Yêu cầu:
1. Xác định bài báo có liên quan đến Hà Nội không
2. Tóm tắt bài báo bằng tiếng Việt, CHÍNH XÁC 100 từ (không nhiều hơn, không ít hơn), giữ lại các thông tin quan trọng nhất về sự kiện, nội dung ngắn gọn, rõ ràng, có cấu trúc dễ đọc. Tập trung vào WH (What, When, Where, Why, How).

Trả về JSON:
{{
    "is_hanoi_related": true/false,
    "summary": "tóm tắt chính xác 100 từ"
}}

Chỉ trả về JSON, không text khác."""

    try:
        response = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Bạn là chuyên gia phân tích và tóm tắt tin tức về Hà Nội. Tóm tắt phải CHÍNH XÁC 100 từ và giữ thông tin quan trọng nhất."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            max_tokens=400,
            response_format={"type": "json_object"}
        )
        
        result = json.loads(response.choices[0].message.content)
        return result
        
    except Exception as e:
        logger.error(f"LLM extraction failed: {e}")
        return None

def calculate_sentence_score(sentence: str, hanoi_keywords: List[str]) -> float:
    """Calculate relevance score for a sentence"""
    score = 0.0
    sentence_lower = sentence.lower()
    
    # Bonus for Hanoi keywords
    for kw in hanoi_keywords:
        if kw.lower() in sentence_lower:
            score += 2.0
    
    # Bonus for containing important info indicators
    important_indicators = [
        'xảy ra', 'diễn ra', 'gây ra', 'dẫn đến', 'khiến',
        'thiệt hại', 'thương vong', 'bị thương', 'tử vong',
        'tai nạn', 'va chạm', 'cháy', 'nổ', 'cướp', 'trộm',
        'kẹt xe', 'ùn tắc', 'ngập', 'lũ lụt', 'sập', 'đổ',
        'theo', 'cho biết', 'thông tin', 'cảnh báo'
    ]
    for indicator in important_indicators:
        if indicator in sentence_lower:
            score += 1.0
    
    # Penalty for very short sentences
    if len(sentence) < 50:
        score -= 2.0
    
    # Penalty for questions
    if '?' in sentence:
        score -= 1.0
    
    return score

def extract_intelligent_summary(title: str, content: str) -> str:
    """Extract intelligent summary based on sentence scoring - 100 từ"""
    
    # Split into sentences
    sentences = re.split(r'[.!?]\s+', content)
    
    # Score each sentence
    scored_sentences = []
    for sent in sentences:
        sent = clean_sentence(sent)
        if is_valid_content_sentence(sent):
            score = calculate_sentence_score(sent, HANOI_KEYWORDS)
            scored_sentences.append((score, sent))
    
    # Sort by score (descending)
    scored_sentences.sort(reverse=True, key=lambda x: x[0])
    
    # Select top sentences up to word limit (100 từ)
    selected_sentences = []
    word_count = 0
    target_words = 100
    
    for score, sent in scored_sentences:
        if word_count >= target_words:
            break
        sent_words = len(sent.split())
        if word_count + sent_words <= target_words + 20:  # Allow slight overflow
            selected_sentences.append((score, sent))
            word_count += sent_words
    
    # Sort selected sentences by their original order in text
    sentence_order = {sent: content.find(sent) for _, sent in selected_sentences}
    selected_sentences.sort(key=lambda x: sentence_order.get(x[1], 999999))
    
    # Build summary
    summary_parts = [sent for _, sent in selected_sentences]
    summary = ". ".join(summary_parts)
    
    # Ensure proper ending
    if summary and not summary.endswith(('.', '!', '?')):
        summary += "."
    
    # Fallback if summary too short
    if len(summary) < 100:
        # Use first few valid sentences
        valid_sents = [s for s in sentences[:8] if is_valid_content_sentence(s)]
        summary = ". ".join(valid_sents[:3])
        if summary and not summary.endswith(('.', '!', '?')):
            summary += "."
    
    # Trim to approximately 100 words if too long
    words = summary.split()
    if len(words) > 120:
        summary = " ".join(words[:100]) + "..."
    
    return summary

def extract_with_rules(title: str, content: str) -> Dict:
    """Enhanced rule-based extraction with intelligent summarization"""
    text = f"{title} {content}".lower()
    
    # Check Hanoi relevance
    is_hanoi = any(kw.lower() in text for kw in HANOI_KEYWORDS)
    if not is_hanoi:
        return {"is_hanoi_related": False}
    
    # Generate intelligent summary (100 từ)
    summary = extract_intelligent_summary(title, content)
    
    return {
        "is_hanoi_related": True,
        "summary": summary
    }

def process_article(article_meta: Dict) -> Optional[Dict]:
    """Process một bài báo hoàn chỉnh"""
    url = article_meta['url']
    
    logger.info(f"Fetching: {url}")
    article_content = extract_article_content(url)
    
    if not article_content:
        return None
    
    title = article_content['title']
    content = article_content['content']
    
    # Try LLM first if available
    if OPENAI_AVAILABLE:
        llm_result = extract_with_llm(title, content)
        if llm_result and llm_result.get('is_hanoi_related'):
            return {
                'title': title,
                'url': url,
                'source': article_meta['source'],
                'date': article_content['publish_date'],
                'summary': llm_result['summary'],
                'content_hash': hashlib.sha256(content.encode()).hexdigest()
            }
    
    # Enhanced rule-based extraction
    rule_result = extract_with_rules(title, content)
    if rule_result.get('is_hanoi_related'):
        return {
            'title': title,
            'url': url,
            'source': article_meta['source'],
            'date': article_content['publish_date'],
            'summary': rule_result['summary'],
            'content_hash': hashlib.sha256(content.encode()).hexdigest()
        }

    return None

def process_article_safe(article_meta: Dict, existing_hashes: set) -> Optional[Dict]:
    """Wrapper để xử lý article an toàn trong thread"""
    try:
        event = process_article(article_meta)
        if event:
            if event['content_hash'] not in existing_hashes:
                return event
        return None
    except Exception as e:
        logger.error(f"Error in thread processing {article_meta.get('url', 'unknown')}: {e}")
        return None

def process_articles_parallel(article_urls: List[Dict], existing_events: List[Dict], max_workers: int = 8) -> List[Dict]:
    """Process articles in parallel - increased to 8 workers for better performance"""
    existing_hashes = {e.get('content_hash') for e in existing_events if e.get('content_hash')}
    
    new_events = []
    completed = 0
    total = len(article_urls)
    
    logger.info(f"Processing {total} articles with {max_workers} workers...")
    
    # Progress indicator
    last_percent = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_article = {
            executor.submit(process_article_safe, article, existing_hashes): article 
            for article in article_urls
        }
        
        for future in as_completed(future_to_article):
            completed += 1
            article = future_to_article[future]
            
            # Terminal progress (simple)
            current_percent = (completed * 100) // total
            if current_percent > last_percent and current_percent % 10 == 0:
                print(f"  Progress: {current_percent}% ({completed}/{total})")
                last_percent = current_percent
            
            try:
                event = future.result()
                if event:
                    new_events.append(event)
                    logger.info(f"[{completed}/{total}] ✓ Crawled: {event['title'][:50]}...")
                else:
                    logger.debug(f"[{completed}/{total}] Skipped: {article.get('url', 'unknown')}")
            except Exception as e:
                logger.error(f"[{completed}/{total}] Error: {e}")
            
            time.sleep(random.uniform(0.5, 1.0))
    
    return new_events

def crawl_rss_feed(source: str, rss_urls: List[str], limit: int = 50) -> List[Dict]:
    """
    Crawl RSS feed to get article URLs - lấy bài trong 24h gần nhất
    FIXED: Better error handling and debug info + HTML fallback
    INCREASED: limit to 50 articles per source
    """
    articles = []
    
    # Try each RSS URL until one works
    for rss_url in rss_urls:
        try:
            logger.info(f"Crawling RSS: {source} - {rss_url}")
            
            # Fetch RSS with proper headers
            response = fetch_url(rss_url, timeout=10)
            if not response:
                logger.warning(f"  Failed to fetch RSS, trying next URL...")
                continue
            
            # Check content type
            content_type = response.headers.get('Content-Type', '').lower()
            logger.info(f"  Content-Type: {content_type}")
            
            # If HTML returned instead of XML, skip to next URL
            if 'text/html' in content_type:
                logger.warning(f"  Received HTML instead of XML/RSS, trying next URL...")
                continue
            
            # Parse feed with sanitization
            feed = feedparser.parse(
                response.content,
                sanitize_html=True,
                resolve_relative_uris=True
            )
            
            # DEBUG: Check feed status
            logger.info(f"  Feed status: {feed.get('status', 'N/A')}")
            logger.info(f"  Feed entries: {len(feed.entries)}")
            
            # Check for serious parsing errors only
            if feed.bozo:
                bozo_exception = str(feed.get('bozo_exception', 'Unknown'))
                # Only warn for non-critical errors
                if 'not well-formed' in bozo_exception or 'syntax error' in bozo_exception:
                    logger.warning(f"  XML syntax warning (may still work): {bozo_exception[:100]}")
                else:
                    logger.warning(f"  Feed parsing warning: {bozo_exception[:100]}")
            
            # If no entries but feed parsed, it might be empty or filtered
            if not feed.entries:
                logger.warning(f"  No entries found (feed may be empty), trying next URL...")
                continue
            
            # Get cutoff time (24 hours ago)
            cutoff_time = datetime.now() - timedelta(hours=24)
            
            processed_count = 0
            for entry in feed.entries[:limit * 5]:  # Lấy nhiều hơn để lọc (5x)
                try:
                    # Parse publish date with multiple fallbacks
                    pub_date = None
                    
                    # Try different date fields
                    if hasattr(entry, 'published_parsed') and entry.published_parsed:
                        try:
                            pub_date = datetime(*entry.published_parsed[:6])
                        except (TypeError, ValueError):
                            pass
                    
                    if not pub_date and hasattr(entry, 'updated_parsed') and entry.updated_parsed:
                        try:
                            pub_date = datetime(*entry.updated_parsed[:6])
                        except (TypeError, ValueError):
                            pass
                    
                    # If still no date, try string parsing
                    if not pub_date:
                        for date_field in ['published', 'updated', 'date']:
                            if hasattr(entry, date_field):
                                date_str = getattr(entry, date_field)
                                try:
                                    from dateutil import parser as date_parser
                                    pub_date = date_parser.parse(date_str)
                                    break
                                except:
                                    pass
                    
                    # DEBUG: Log first few entries
                    if processed_count < 3:
                        logger.debug(f"  Entry: {entry.get('title', 'No title')[:50]}...")
                        logger.debug(f"    Date: {pub_date}")
                        logger.debug(f"    Link: {entry.get('link', 'No link')}")
                    
                    processed_count += 1
                    
                    # Get link
                    link = entry.get('link', '')
                    title = entry.get('title', 'No title')
                    
                    if not link:
                        continue
                    
                    # Lấy bài trong 24h gần nhất (hoặc không có ngày)
                    if pub_date and pub_date >= cutoff_time:
                        articles.append({
                            'url': link,
                            'title': title,
                            'source': source,
                            'pub_date': pub_date
                        })
                    elif not pub_date:
                        # If no date, include it anyway (assume recent)
                        articles.append({
                            'url': link,
                            'title': title,
                            'source': source,
                            'pub_date': datetime.now()
                        })
                    
                    if len(articles) >= limit:
                        break
                        
                except Exception as e:
                    logger.debug(f"  Error parsing entry: {e}")
                    continue
            
            logger.info(f"  Processed {processed_count} entries, found {len(articles)} recent articles")
            
            # If we got articles, no need to try other URLs
            if articles:
                logger.info(f"✓ Found {len(articles)} articles from last 24h for {source}")
                break
            else:
                logger.warning(f"  No recent articles found, trying next URL...")
                
        except Exception as e:
            logger.error(f"Error crawling RSS {source} ({rss_url}): {e}")
            continue
    
    # Final result
    if not articles:
        logger.warning(f"✗ No articles found for {source} from any RSS URL")
    
    return articles

def main():
    """Main crawler"""
    print("="*60)
    print("SafeMap Crawler - Running...")
    print("="*60)
    print("✓ Logs: crawler.log")
    print("✓ Output: safemap_data.json")
    print("="*60 + "\n")
    
    logger.info("="*60)
    logger.info("SafeMap Crawler - Enhanced with RSS Debug")
    logger.info("="*60)
    
    # Load existing events
    existing_events = []
    
    # Thư mục dự án (SAFEMAP)
    PROJECT_ROOT = Path(__file__).resolve().parents[1]   # ../ từ Xu_li_data

    # Thư mục Data và các file đích
    DATA_DIR = PROJECT_ROOT / "Data"
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    output_file = DATA_DIR / "safemap_data.json"
    LOG_FILE    = DATA_DIR / "crawler.log"
    
    try:
        with open(output_file, 'r', encoding='utf-8') as f:
            content = f.read().strip()
            if content:
                existing_events = json.loads(content)
            else:
                logger.info("JSON file is empty, starting fresh")
        logger.info(f"Loaded {len(existing_events)} existing events")
    except FileNotFoundError:
        logger.info("No existing data file, starting fresh")
    except json.JSONDecodeError as e:
        logger.warning(f"Invalid JSON in existing file: {e}. Starting fresh")
        import shutil
        backup_file = f"{output_file}.backup"
        try:
            shutil.copy(output_file, backup_file)
            logger.info(f"Backed up corrupted file to {backup_file}")
        except:
            pass
    
    # Collect URLs from RSS
    print("Phase 1: Collecting RSS feeds...")
    logger.info("\n" + "="*60)
    logger.info("PHASE 1: Collecting URLs from RSS feeds")
    logger.info("="*60)
    
    all_article_urls = []
    source_stats = {}
    
    for source, config in SOURCES.items():
        articles = crawl_rss_feed(source, config['rss'], limit=50)  # Increased to 50
        all_article_urls.extend(articles)
        source_stats[source] = len(articles)
        time.sleep(1)  # Delay between sources
    
    # Print summary to terminal (simplified)
    print(f"\n✓ Collected {len(all_article_urls)} articles from {len([c for c in source_stats.values() if c > 0])} sources")
    
    # Full summary to log file
    logger.info("\n" + "-"*60)
    logger.info("RSS Collection Summary:")
    for source, count in source_stats.items():
        logger.info(f"  {source}: {count} articles")
    logger.info(f"Total URLs collected: {len(all_article_urls)}")
    logger.info("-"*60 + "\n")
    
    if not all_article_urls:
        print("✗ No articles collected! Check crawler.log for details.")
        logger.error("No articles collected! Check RSS URLs and network connection.")
        return
    
    # Process articles in parallel
    print(f"\nPhase 2: Processing {len(all_article_urls)} articles...")
    logger.info("\n" + "="*60)
    logger.info("PHASE 2: Processing articles")
    logger.info("="*60)
    
    new_events = process_articles_parallel(all_article_urls, existing_events, max_workers=8)
    
    # Merge and save
    all_events = existing_events + new_events
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(all_events, f, ensure_ascii=False, indent=2)
    
    # Terminal summary (simple)
    print(f"\n{'='*60}")
    print(f"✓ Finished!")
    print(f"  New events: {len(new_events)}")
    print(f"  Total events: {len(all_events)}")
    print(f"  Saved to: {output_file}")
    print(f"{'='*60}\n")
    
    # Detailed log
    logger.info("\n" + "="*60)
    logger.info(f"Finished: {len(new_events)} new events added")
    logger.info(f"Total events: {len(all_events)}")
    logger.info("="*60)

if __name__ == "__main__":
    main()

