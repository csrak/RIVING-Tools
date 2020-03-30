import requests
import lxml.html as lh
import pandas as pd
import os
import glob
from bs4 import BeautifulSoup
from pathlib import Path
from difflib import SequenceMatcher
import re
import tabula #install tabula-py
import PyPDF2
import Chile_Data as CL
from zipfile import ZipFile