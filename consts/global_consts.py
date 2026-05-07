import random
import kbt_load_env
from datetime import datetime
from datetime import timedelta
from datetime import date
import base64
# all kbt CONSTANT variables indicated with capital snake case

#Wordpress constants
WP_LOCAL_URL = 'http://localhost:8080/wordpress/wp-json/wp/v2/posts/?post_type=predictions'
WP_LIVE_URL = 'https://kingsbettingtips.com/wp-json/wp/v2/posts'
WP_POST_TIME = datetime.now() + timedelta(minutes=5)
WP_POST_DATE = WP_POST_TIME.strftime("%Y-%m-%dT%H:%M:%S")
WP_LOCAL_USER = 'root'
WP_LIVE_USER = 'adminKBT'
WP_LOCAL_PASSWORD = kbt_load_env.local_pwd
WP_LIVE_PASSWORD = kbt_load_env.live_pwd
WP_CREDITIALS = WP_LIVE_USER + ':' + WP_LIVE_PASSWORD
WP_TOKEN = base64.b64encode(WP_CREDITIALS.encode())
WP_HEADER = {'Authorization': 'Basic ' + WP_TOKEN.decode('utf-8')}


WP_WELCOME_NOTE = """ Hello Kings!

We have just released a new set of football prediction tips for the upcoming matches. Our team of experts have analyzed the data and have come up with some highly accurate predictions.

Whether you are a casual fan or a serious bettor, these tips will help you make informed decisions about your bets. Be sure to check them out and let us know what you think.
<blockquote>
<small>As always, it is important to remember that these tips are not guaranteed and that there is always a risk of losing money when betting.</small> <strong>Please gamble responsibly.</strong></blockquote>

Thank you for your support and happy betting!<br> """

WP_JOIN_TELEGRAM_NOTE = """ We have recently started a new Telegram group for sports fans to discuss and share their thoughts on the latest events and games.
If you are interested in joining, please click on the link below to request access. We look forward to having you as a member of our community.
See you in the group!<br> 
<div class="media single-contact-info">
<a class="btn btn-base" href="https://t.me/+EQVAXh9ctNgwZDJk">Join our Telegram Group<i class="fas fa-arrow-alt-circle-right ms-2"></i></a>
        </div>  
"""

WP_COMMENT_NOTE = """ <p>
We would love to hear your thoughts and opinions on it. Please feel free to leave a comment below and let us know what you think. 
Your feedback is always valuable to us. Thank you!</p> """




#Time constants
MYSQL_DATE = date.today().strftime('%Y-%m-%d')

PRESENT_DAY_DATE = date.today()
PRESENT_DAY_DMY = date.today().strftime('%d-%m-%Y')
PRESENT_DAY_YMD = date.today().strftime('%Y-%m-%d')

YESTERDAY_DATE = PRESENT_DAY_DATE - timedelta(days=1)
YESTERDAY_YMD = YESTERDAY_DATE.strftime('%Y-%m-%d')  # Correct format for MySQL
YESTERDAY_DMY = YESTERDAY_DATE.strftime('%d-%m-%Y')  # Correct format for MySQL


TOMORROW_YMD = (date.today() + timedelta(days=1)).strftime('%Y-%m-%d')


#header constants
# Define a list of header dictionaries for different browsers
headers_list = [
    
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"},
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"},
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"},
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"},
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"},
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) Gecko/20100101 Firefox/91.0", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"},
    {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"},
    {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.2 Safari/605.1.15", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"},
    {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.2 Safari/605.1.15", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"},
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edg/91.0.864.59", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"},
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edg/92.0.902.55", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"},
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edg/93.0.961.38", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"},
    {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36", "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8"},
    {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:64.0) Gecko/20100101 Firefox/64.0", "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"},
    {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:71.0) Gecko/20100101 Firefox/71.0", "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"},
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"},
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:88.0) Gecko/20100101 Firefox/88.0"},
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.818.62 Safari/537.36 Edg/90.0.818.62"},
    {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"},
    {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7; rv:88.0) Gecko/20100101 Firefox/88.0"},
    {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15"},
    {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"},
    {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0"},
    { "User-Agent": "Mozilla/5.0 (Linux; Android 10; SM-G970F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Mobile Safari/537.36"},
    {"User-Agent": "Mozilla/5.0 (Android 10; Mobile; rv:88.0) Gecko/88.0 Firefox/88.0" },
    {"User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1" },
    {"User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Mobile Safari/537.36"},
    {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36", "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8"}
]
# Additional headers
additional_headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.google.com",
    "Connection": "keep-alive"
}
# Select a random header from the list
import random  # make sure this is at top

headers = random.choice(headers_list)

# Merge properly
headers.update(additional_headers)

MY_HEARDER = headers  # ✅ NOW it's a valid dict
 

#cvs file names


PRO_RESULTS_CSV = "csv_files/pro_results.csv"
PRO_CSV = "csv_files/pro_tips.csv"
API_FOOTBALL_RESULTS_CSV = "csv_files/api_call_pro_results.csv"
API_FOOTBALL_CSV = "csv_files/api_match.csv"
MATCHED_RESULTS_CSV = "csv_files/matched_results.csv"
MATCHED_FIXTURES_CSV = "csv_files/matched_fixtures.csv"




#telegram const
# Replace 'YOUR_BOT_TOKEN' with your actual bot token
BOT_TOKEN = '6151016586:AAHnkIqKqBO01v1fQfd8noSOCzKLHjrWyGw'

# Replace 'CHANNEL_CHAT_ID' with the actual chat ID of your channel
CHANNEL_CHAT_ID = '-1001701327610'

# URL for the Telegram Bot API
# BASE_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
# URL for the Telegram Bot API
# URL for the Telegram Bot API
TELEGRAM_BASE_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/"



COUNTRIES = [
    #LEAGUES
     {"2_code": "gb-eng", "3_code": "EPL", "name": "England"},
     {"2_code": "gb-eng", "3_code": "EFL", "name": "England"},
     {"2_code": "DE", "3_code": "GER", "name": "Germany"},

    #COUNTRIES
    {"2_code": "AF", "3_code": "AFG", "name": "Afghanistan"},
    {"2_code": "AX", "3_code": "ALA", "name": "Åland Islands"},
    {"2_code": "AL", "3_code": "ALB", "name": "Albania"},
    {"2_code": "DZ", "3_code": "DZA", "name": "Algeria"},
    {"2_code": "AS", "3_code": "ASM", "name": "American Samoa"},
    {"2_code": "AD", "3_code": "AND", "name": "Andorra"},
    {"2_code": "AO", "3_code": "AGO", "name": "Angola"},
    {"2_code": "AI", "3_code": "AIA", "name": "Anguilla"},
    {"2_code": "", "3_code": "ATA", "name": "Antarctica"},
    {"2_code": "AG", "3_code": "ATG", "name": "Antigua and Barbuda"},
    {"2_code": "AR", "3_code": "ARG", "name": "Argentina"},
    {"2_code": "AM", "3_code": "ARM", "name": "Armenia"},
    {"2_code": "AW", "3_code": "ABW", "name": "Aruba"},
    {"2_code": "AU", "3_code": "AUS", "name": "Australia"},
    {"2_code": "AT", "3_code": "AUT", "name": "Austria"},
    {"2_code": "AZ", "3_code": "AZE", "name": "Azerbaijan"},
    {"2_code": "BS", "3_code": "BHS", "name": "Bahamas"},
    {"2_code": "BH", "3_code": "BHR", "name": "Bahrain"},
    {"2_code": "BD", "3_code": "BGD", "name": "Bangladesh"},
    {"2_code": "BB", "3_code": "BRB", "name": "Barbados"},
    {"2_code": "BY", "3_code": "BLR", "name": "Belarus"},
    {"2_code": "BE", "3_code": "BEL", "name": "Belgium"},
    {"2_code": "BZ", "3_code": "BLZ", "name": "Belize"},
    {"2_code": "BJ", "3_code": "BEN", "name": "Benin"},
    {"2_code": "BM", "3_code": "BMU", "name": "Bermuda"},
    {"2_code": "BT", "3_code": "BTN", "name": "Bhutan"},
    {"2_code": "BO", "3_code": "BOL", "name": "Bolivia, Plurinational State of"},
    {"2_code": "", "3_code": "BES", "name": "Bonaire, Sint Eustatius and Saba"},
    {"2_code": "BA", "3_code": "BIH", "name": "Bosnia and Herzegovina"},
    {"2_code": "BW", "3_code": "BWA", "name": "Botswana"},
    {"2_code": "", "3_code": "BVT", "name": "Bouvet Island"},
    {"2_code": "BR", "3_code": "BRA", "name": "Brazil"},
    {"2_code": "IO", "3_code": "IOT", "name": "British Indian Ocean Territory"},
    {"2_code": "BN", "3_code": "BRN", "name": "Brunei Darussalam"},
    {"2_code": "BG", "3_code": "BGR", "name": "Bulgaria"},
    {"2_code": "BG", "3_code": "BUL", "name": "Bulgaria"},
    {"2_code": "BF", "3_code": "BFA", "name": "Burkina Faso"},
    {"2_code": "BI", "3_code": "BDI", "name": "Burundi"},
    {"2_code": "KH", "3_code": "KHM", "name": "Cambodia"},
    {"2_code": "CM", "3_code": "CMR", "name": "Cameroon"},
    {"2_code": "CA", "3_code": "CAN", "name": "Canada"},
    {"2_code": "CV", "3_code": "CPV", "name": "Cape Verde"},
    {"2_code": "KY", "3_code": "CYM", "name": "Cayman Islands"},
    {"2_code": "CF", "3_code": "CAF", "name": "Central African Republic"},
    {"2_code": "TD", "3_code": "TCD", "name": "Chad"},
    {"2_code": "CL", "3_code": "CHL", "name": "Chile"},
    {"2_code": "CN", "3_code": "CHN", "name": "China"},
    {"2_code": "CX", "3_code": "CXR", "name": "Christmas Island"},
    {"2_code": "CC", "3_code": "CCK", "name": "Cocos (Keeling) Islands"},
    {"2_code": "CO", "3_code": "COL", "name": "Colombia"},
    {"2_code": "KM", "3_code": "COM", "name": "Comoros"},
    {"2_code": "CG", "3_code": "COG", "name": "Congo"},
    {"2_code": "CD", "3_code": "COD", "name": "Congo, the Democratic Republic of the"},
    {"2_code": "CK", "3_code": "COK", "name": "Cook Islands"},
    {"2_code": "CR", "3_code": "CRI", "name": "Costa Rica"},
    {"2_code": "CI", "3_code": "CIV", "name": "Côte d'Ivoire"},
    {"2_code": "HR", "3_code": "HRV", "name": "Croatia"},
    {"2_code": "CU", "3_code": "CUB", "name": "Cuba"},
    {"2_code": "CW", "3_code": "CUW", "name": "Curaçao"},
    {"2_code": "CY", "3_code": "CYP", "name": "Cyprus"},
    {"2_code": "CZ", "3_code": "CZE", "name": "Czech Republic"},
    {"2_code": "DK", "3_code": "DNK", "name": "Denmark"},
    {"2_code": "DK", "3_code": "DEN", "name": "Denmark"},

    {"2_code": "DJ", "3_code": "DJI", "name": "Djibouti"},
    {"2_code": "DM", "3_code": "DMA", "name": "Dominica"},
    {"2_code": "DO", "3_code": "DOM", "name": "Dominican Republic"},
    {"2_code": "EC", "3_code": "ECU", "name": "Ecuador"},
    {"2_code": "EG", "3_code": "EGY", "name": "Egypt"},
    {"2_code": "SV", "3_code": "SLV", "name": "El Salvador"},
    {"2_code": "GQ", "3_code": "GNQ", "name": "Equatorial Guinea"},
    {"2_code": "ER", "3_code": "ERI", "name": "Eritrea"},
    {"2_code": "EE", "3_code": "EST", "name": "Estonia"},
    {"2_code": "ET", "3_code": "ETH", "name": "Ethiopia"},
    {"2_code": "", "3_code": "FLK", "name": "Falkland Islands (Malvinas)"},
    {"2_code": "", "3_code": "FRO", "name": "Faroe Islands"},
    {"2_code": "FJ", "3_code": "FJI", "name": "Fiji"},
    {"2_code": "FI", "3_code": "FIN", "name": "Finland"},
    {"2_code": "FR", "3_code": "FRA", "name": "France"},
    {"2_code": "FR", "3_code": "FL2", "name": "France"},
    {"2_code": "", "3_code": "GUF", "name": "French Guiana"},
    {"2_code": "", "3_code": "PYF", "name": "French Polynesia"},
    {"2_code": "", "3_code": "ATF", "name": "French Southern Territories"},
    {"2_code": "GA", "3_code": "GAB", "name": "Gabon"},
    {"2_code": "GM", "3_code": "GMB", "name": "Gambia"},
    {"2_code": "GE", "3_code": "GEO", "name": "Georgia"},
    {"2_code": "DE", "3_code": "DEU", "name": "Germany"},
    {"2_code": "GH", "3_code": "GHA", "name": "Ghana"},
    {"2_code": "GI", "3_code": "GIB", "name": "Gibraltar"},
    {"2_code": "GR", "3_code": "GRC", "name": "Greece"},
    {"2_code": "", "3_code": "GRL", "name": "Greenland"},
    {"2_code": "GD", "3_code": "GRD", "name": "Grenada"},
    {"2_code": "", "3_code": "GLP", "name": "Guadeloupe"},
    {"2_code": "", "3_code": "GUM", "name": "Guam"},
    {"2_code": "GT", "3_code": "GTM", "name": "Guatemala"},
    {"2_code": "", "3_code": "GGY", "name": "Guernsey"},
    {"2_code": "GN", "3_code": "GIN", "name": "Guinea"},
    {"2_code": "GW", "3_code": "GNB", "name": "Guinea-Bissau"},
    {"2_code": "GY", "3_code": "GUY", "name": "Guyana"},
    {"2_code": "HT", "3_code": "HTI", "name": "Haiti"},
    {"2_code": "", "3_code": "HMD", "name": "Heard Island and McDonald Islands"},
    {"2_code": "VA", "3_code": "VAT", "name": "Holy See (Vatican City State)"},
    {"2_code": "HN", "3_code": "HND", "name": "Honduras"},
    {"2_code": "HK", "3_code": "HKG", "name": "Hong Kong"},
    {"2_code": "HU", "3_code": "HUN", "name": "Hungary"},
    {"2_code": "IS", "3_code": "ISL", "name": "Iceland"},
    {"2_code": "IN", "3_code": "IND", "name": "India"},
    {"2_code": "ID", "3_code": "IDN", "name": "Indonesia"},
    {"2_code": "IR", "3_code": "IRN", "name": "Iran, Islamic Republic of"},
    {"2_code": "IQ", "3_code": "IRQ", "name": "Iraq"},
    {"2_code": "IE", "3_code": "IRL", "name": "Ireland"},
    {"2_code": "", "3_code": "IMN", "name": "Isle of Man"},
    {"2_code": "IL", "3_code": "ISR", "name": "Israel"},
    {"2_code": "IT", "3_code": "ITA", "name": "Italy"},
    {"2_code": "JM", "3_code": "JAM", "name": "Jamaica"},
    {"2_code": "JP", "3_code": "JPN", "name": "Japan"},
    {"2_code": "JP", "3_code": "JAP", "name": "Japan"},
    {"2_code": "", "3_code": "JEY", "name": "Jersey"},
    {"2_code": "JO", "3_code": "JOR", "name": "Jordan"},
    {"2_code": "KZ", "3_code": "KAZ", "name": "Kazakhstan"},
    {"2_code": "KE", "3_code": "KEN", "name": "Kenya"},
    {"2_code": "KI", "3_code": "KIR", "name": "Kiribati"},
    {"2_code": "KP", "3_code": "PRK", "name": "Korea, Democratic People's Republic of"},
    {"2_code": "KR", "3_code": "KOR", "name": "Korea, Republic of"},
    {"2_code": "KW", "3_code": "KWT", "name": "Kuwait"},
    {"2_code": "KG", "3_code": "KGZ", "name": "Kyrgyzstan"},
    {"2_code": "LA", "3_code": "LAO", "name": "Lao People's Democratic Republic"},
    {"2_code": "LV", "3_code": "LVA", "name": "Latvia"},

    {"2_code": "LY", "3_code": "LBY", "name": "Libya"},
    {"2_code": "MA", "3_code": "MAR", "name": "Morocco"},
    {"2_code": "MC", "3_code": "MCO", "name": "Monaco"},
    {"2_code": "MD", "3_code": "MDA", "name": "Moldova"},
    {"2_code": "ME", "3_code": "MNE", "name": "Montenegro"},
    {"2_code": "MF", "3_code": "MAF", "name": "Saint Martin (French part)"},
    {"2_code": "MG", "3_code": "MDG", "name": "Madagascar"},
    {"2_code": "MH", "3_code": "MHL", "name": "Marshall Islands"},
    {"2_code": "MK", "3_code": "MKD", "name": "North Macedonia"},
    {"2_code": "ML", "3_code": "MLI", "name": "Mali"},
    {"2_code": "MM", "3_code": "MMR", "name": "Myanmar"},
    {"2_code": "MN", "3_code": "MNG", "name": "Mongolia"},
    {"2_code": "MO", "3_code": "MAC", "name": "Macau"},
    {"2_code": "MP", "3_code": "MNP", "name": "Northern Mariana Islands"},
    {"2_code": "MQ", "3_code": "MTQ", "name": "Martinique"},
    {"2_code": "MR", "3_code": "MRT", "name": "Mauritania"},
    {"2_code": "MS", "3_code": "MSR", "name": "Montserrat"},
    {"2_code": "MT", "3_code": "MLT", "name": "Malta"},
    {"2_code": "MU", "3_code": "MUS", "name": "Mauritius"},
    {"2_code": "MV", "3_code": "MDV", "name": "Maldives"},
    {"2_code": "MW", "3_code": "MWI", "name": "Malawi"},
    {"2_code": "MX", "3_code": "MEX", "name": "Mexico"},
    {"2_code": "MY", "3_code": "MYS", "name": "Malaysia"},
    {"2_code": "MZ", "3_code": "MOZ", "name": "Mozambique"},
    {"2_code": "NA", "3_code": "NAM", "name": "Namibia"},
    {"2_code": "NC", "3_code": "NCL", "name": "New Caledonia"},
    {"2_code": "NE", "3_code": "NER", "name": "Niger"},
    {"2_code": "NF", "3_code": "NFK", "name": "Norfolk Island"},
    {"2_code": "NG", "3_code": "NGA", "name": "Nigeria"},
    {"2_code": "NI", "3_code": "NIC", "name": "Nicaragua"},
    {"2_code": "NL", "3_code": "NLD", "name": "Netherlands"},
    {"2_code": "NL", "3_code": "NED", "name": "Netherlands"},
    {"2_code": "NO", "3_code": "NOR", "name": "Norway"},
    {"2_code": "NP", "3_code": "NPL", "name": "Nepal"},
    {"2_code": "NR", "3_code": "NRU", "name": "Nauru"},
    {"2_code": "NU", "3_code": "NIU", "name": "Niue"},
    {"2_code": "NZ", "3_code": "NZL", "name": "New Zealand"},
    {"2_code": "OM", "3_code": "OMN", "name": "Oman"},
    {"2_code": "PA", "3_code": "PAN", "name": "Panama"},
    {"2_code": "PE", "3_code": "PER", "name": "Peru"},
    {"2_code": "PF", "3_code": "PYF", "name": "French Polynesia"},
    {"2_code": "PG", "3_code": "PNG", "name": "Papua New Guinea"},
    {"2_code": "PH", "3_code": "PHL", "name": "Philippines"},
    {"2_code": "PK", "3_code": "PAK", "name": "Pakistan"},
    {"2_code": "PL", "3_code": "POL", "name": "Poland"},
    {"2_code": "PM", "3_code": "PMT", "name": "Saint Pierre and Miquelon"},
    {"2_code": "PN", "3_code": "PCN", "name": "Pitcairn Islands"},
    {"2_code": "PR", "3_code": "PRI", "name": "Puerto Rico"},
    {"2_code": "PS", "3_code": "PSE", "name": "Palestine"},
    {"2_code": "PT", "3_code": "PRT", "name": "Portugal"},
    {"2_code": "PW", "3_code": "PLW", "name": "Palau"},
    {"2_code": "PY", "3_code": "PRY", "name": "Paraguay"},
    {"2_code": "QA", "3_code": "QAT", "name": "Qatar"},
    {"2_code": "RE", "3_code": "REU", "name": "Réunion"},
    {"2_code": "RO", "3_code": "ROU", "name": "Romania"},
    {"2_code": "RU", "3_code": "RUS", "name": "Russia"},
    {"2_code": "SH", "3_code": "SHN", "name": "Saint Helena, Ascension and Tristan da Cunha"},
    {"2_code": "KN", "3_code": "KNA", "name": "Saint Kitts and Nevis"},
    {"2_code": "LC", "3_code": "LCA", "name": "Saint Lucia"},
    {"2_code": "", "3_code": "MAF", "name": "Saint Martin (French part)"},
    {"2_code": "", "3_code": "SPM", "name": "Saint Pierre and Miquelon"},
    {"2_code": "VC", "3_code": "VCT", "name": "Saint Vincent and the Grenadines"},
    {"2_code": "WS", "3_code": "WSM", "name": "Samoa"},
    {"2_code": "SM", "3_code": "SMR", "name": "San Marino"},
    {"2_code": "ST", "3_code": "STP", "name": "Sao Tome and Principe"},
    {"2_code": "SA", "3_code": "SAU", "name": "Saudi Arabia"},
    {"2_code": "SN", "3_code": "SEN", "name": "Senegal"},
    {"2_code": "RS", "3_code": "SRB", "name": "Serbia"},
    {"2_code": "SC", "3_code": "SYC", "name": "Seychelles"},
    {"2_code": "SL", "3_code": "SLE", "name": "Sierra Leone"},
    {"2_code": "SG", "3_code": "SGP", "name": "Singapore"},
    {"2_code": "", "3_code": "SXM", "name": "Sint Maarten (Dutch part)"},
    {"2_code": "SK", "3_code": "SVK", "name": "Slovakia"},
    {"2_code": "SI", "3_code": "SVN", "name": "Slovenia"},
    {"2_code": "SB", "3_code": "SLB", "name": "Solomon Islands"},
    {"2_code": "SO", "3_code": "SOM", "name": "Somalia"},
    {"2_code": "ZA", "3_code": "ZAF", "name": "South Africa"},
    {"2_code": "", "3_code": "SGS", "name": "South Georgia and the South Sandwich Islands"},
    {"2_code": "SS", "3_code": "SSD", "name": "South Sudan"},
    {"2_code": "ES", "3_code": "ESP", "name": "Spain"},
    {"2_code": "LK", "3_code": "LKA", "name": "Sri Lanka"},
    {"2_code": "SD", "3_code": "SDN", "name": "Sudan"},
    {"2_code": "SR", "3_code": "SUR", "name": "Suriname"},
    {"2_code": "", "3_code": "SJM", "name": "Svalbard and Jan Mayen"},
    {"2_code": "SZ", "3_code": "SWZ", "name": "Swaziland"},
    {"2_code": "SE", "3_code": "SWE", "name": "Sweden"},
    {"2_code": "CH", "3_code": "CHE", "name": "Switzerland"},
    {"2_code": "SY", "3_code": "SYR", "name": "Syrian Arab Republic"},
    {"2_code": "TW", "3_code": "TWN", "name": "Taiwan, Province of China"},
    {"2_code": "TJ", "3_code": "TJK", "name": "Tajikistan"},
    {"2_code": "TZ", "3_code": "TZA", "name": "Tanzania, United Republic of"},
    {"2_code": "TH", "3_code": "THA", "name": "Thailand"},
    {"2_code": "TL", "3_code": "TLS", "name": "Timor-Leste"},
    {"2_code": "TG", "3_code": "TGO", "name": "Togo"},
    {"2_code": "TK", "3_code": "TKL", "name": "Tokelau"},
    {"2_code": "TO", "3_code": "TON", "name": "Tonga"},
    {"2_code": "TT", "3_code": "TTO", "name": "Trinidad and Tobago"},
    {"2_code": "TN", "3_code": "TUN", "name": "Tunisia"},
    {"2_code": "TR", "3_code": "TUR", "name": "Turkey"},
    {"2_code": "TM", "3_code": "TKM", "name": "Turkmenistan"},
    {"2_code": "TC", "3_code": "TCA", "name": "Turks and Caicos Islands"},
    {"2_code": "TV", "3_code": "TUV", "name": "Tuvalu"},
    {"2_code": "UG", "3_code": "UGA", "name": "Uganda"},
    {"2_code": "UA", "3_code": "UKR", "name": "Ukraine"},
    {"2_code": "AE", "3_code": "ARE", "name": "United Arab Emirates"},
    {"2_code": "GB", "3_code": "GBR", "name": "United Kingdom"},
    {"2_code": "US", "3_code": "USA", "name": "United States"},
    {"2_code": "", "3_code": "UMI", "name": "United States Minor Outlying Islands"},
    {"2_code": "UY", "3_code": "URY", "name": "Uruguay"},
    {"2_code": "UZ", "3_code": "UZB", "name": "Uzbekistan"},
    {"2_code": "VU", "3_code": "VUT", "name": "Vanuatu"},
    {"2_code": "VE", "3_code": "VEN", "name": "Venezuela, Bolivarian Republic of"},
    {"2_code": "VN", "3_code": "VNM", "name": "Viet Nam"},
    {"2_code": "", "3_code": "VGB", "name": "Virgin Islands, British"},
    {"2_code": "", "3_code": "VIR", "name": "Virgin Islands, U.S."},
    {"2_code": "", "3_code": "WLF", "name": "Wallis and Futuna"},
    {"2_code": "EH", "3_code": "ESH", "name": "Western Sahara"},
    {"2_code": "YE", "3_code": "YEM", "name": "Yemen"},
    {"2_code": "ZM", "3_code": "ZMB", "name": "Zambia"},
    {"2_code": "ZW", "3_code": "ZWE", "name": "Zimbabwe"}
]














