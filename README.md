# datamule-indicators

Analytics MVP using the entire SEC: [indicators](https://datamule.xyz/indicators)

## mentions
Exact mentions found in SEC documents. May be boilerplate. [Suggest More](https://github.com/john-friedman/datamule-indicators/issues/1)

## holdings

### 345
- start 2010, go to 2025.
- stock from xbrl (dei and us-gaap)
- individual stock from 345. direct and indirect (via trusts) calculated.
- gender predicted using https://github.com/lead-ratings/gender-guesser
- ethnicity (might do later)

Note: The code here is messy. Some of the functions used are from my local version of [datamule](https://github.com/john-friedman/datamule-python), and may not be uploaded yet. Will clean this up over time. 

## Todo
1. Might zstandard compress the csvs to check if it makes browser faster.