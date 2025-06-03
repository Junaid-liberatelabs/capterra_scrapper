from datetime import datetime
from typing import Optional, Tuple

def parse_capterra_datetime_for_output(date_str: str) -> Tuple[Optional[str], Optional[datetime]]:
    if not date_str:
        return None, None
    parsed_dt = None
    formats_to_try = ["%B %d, %Y", "%b %d, %Y", "%d %B %Y", "%d %b %Y"]
    for fmt in formats_to_try:
        try:
            parsed_dt = datetime.strptime(date_str.strip(), fmt)
            break
        except ValueError:
            continue
    if parsed_dt:
        output_str = parsed_dt.strftime("%Y-%m-%d") + " 00:00:00 +0000"
        return output_str, parsed_dt
    return date_str, None # Return original string if parsing failed