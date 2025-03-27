import urllib.parse

def create_text_fragment_url(url, query_list):
    # Initialize empty list to hold all fragments
    all_fragments = []
    
    # Process each query in the list
    for query in query_list:
        # Split the query into separate terms and clean them
        terms = [term.strip() for term in query.split('OR')]
        
        # Process each term in the current query
        for term in terms:
            # Remove any quotes and clean whitespace
            clean_term = term.strip().replace('"', '').strip()
            # URL encode the term
            encoded_term = urllib.parse.quote(clean_term)
            all_fragments.append(f"text={encoded_term}")
    
    # Join all fragments with &
    full_fragment = "&".join(all_fragments)
    
    # Append the fragment to the URL
    if '#' in url:
        # URL already has a fragment, need to handle carefully
        base_url, existing_fragment = url.split('#', 1)
        result_url = f"{base_url}#:~:{full_fragment}&:~:{existing_fragment}"
    else:
        result_url = f"{url}#:~:{full_fragment}"
    
    return result_url