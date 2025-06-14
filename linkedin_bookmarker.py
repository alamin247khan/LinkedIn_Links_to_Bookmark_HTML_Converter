#!/usr/bin/env python3
"""
LinkedIn Links to HTML Bookmarks Converter
Takes raw LinkedIn URLs from any source and creates organized browser bookmarks
"""

import os
import re
from datetime import datetime
from urllib.parse import urlparse, urljoin

def clean_linkedin_url(url):
    """Normalize LinkedIn URLs to consistent format"""
    url = url.strip()
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    
    # Remove tracking parameters and fragments
    parsed = urlparse(url)
    clean_url = urljoin(f"{parsed.scheme}://{parsed.netloc}", parsed.path)
    return clean_url.split('?')[0]

def extract_profile_info(url):
    """Extract basic info from LinkedIn URL"""
    # Get profile name from URL
    profile_name = url.split('/in/')[-1].split('/')[0]
    return {
        'name': ' '.join([word.capitalize() for word in profile_name.split('-')]),
        'url': url
    }

def generate_bookmark_html(profiles, output_file):
    """Generate Netscape-format bookmark HTML"""
    timestamp = int(datetime.now().timestamp())
    
    html = [
        '<!DOCTYPE NETSCAPE-Bookmark-file-1>',
        '<META HTTP-EQUIV="Content-Type" CONTENT="text/html; charset=UTF-8">',
        '<TITLE>LinkedIn Bookmarks</TITLE>',
        '<H1>LinkedIn Bookmarks</H1>',
        '<DL><p>'
    ]
    
    # Group by first letter of last name
    groups = {}
    for profile in profiles:
        last_initial = profile['name'].split()[-1][0].upper()
        groups.setdefault(last_initial, []).append(profile)
    
    for initial in sorted(groups.keys()):
        html.append(f'    <DT><H3>{initial}</H3>')
        html.append('    <DL><p>')
        
        for profile in sorted(groups[initial], key=lambda x: x['name']):
            html.append(f'        <DT><A HREF="{profile["url"]}">{profile["name"]}</A>')
        
        html.append('    </DL><p>')
    
    html.append('</DL><p>')
    
    with open(output_file, 'w') as f:
        f.write('\n'.join(html))

def process_input_file(input_file):
    """Extract LinkedIn URLs from any text file"""
    with open(input_file, 'r') as f:
        text = f.read()
    
    # Find all LinkedIn profile URLs
    pattern = r'(https?://(?:www\.)?linkedin\.com/in/[^\s]+)'
    urls = re.findall(pattern, text)
    return [clean_linkedin_url(url) for url in urls]

def main():
    """Command line interface"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Convert LinkedIn URLs to HTML bookmarks')
    parser.add_argument('input', help='Input file containing LinkedIn URLs')
    parser.add_argument('-o', '--output', default='linkedin_bookmarks.html',
                      help='Output HTML file name')
    
    args = parser.parse_args()
    
    # Process input
    urls = process_input_file(args.input)
    profiles = [extract_profile_info(url) for url in urls]
    
    # Generate output
    generate_bookmark_html(profiles, args.output)
    print(f"Success! Created bookmark file with {len(profiles)} profiles: {args.output}")

if __name__ == '__main__':
    main()