#import openai, os
from PyPDF2 import PdfReader
import os
import tiktoken

# Load the tokenizer for the specific model you plan to use
encoding = tiktoken.encoding_for_model("gpt-4o")


def extract_text_from_pdf(pdf_path, start_page=0, end_page=None):
    """
    Extract text from a specific range of pages in a PDF.

    Parameters:
    pdf_path (str): The path to the PDF file.
    start_page (int): The page to start extracting text from (0-indexed).
    end_page (int): The page to stop extracting text at (0-indexed, inclusive). If None, extracts to the end.

    Returns:
    str: Extracted text from the specified page range.
    """
    reader = PdfReader(pdf_path)
    text = ""

    # Ensure end_page is within the number of pages
    if end_page is None or end_page >= len(reader.pages):
        end_page = len(reader.pages) - 1

    # Extract text from the specified range of pages
    for page_num in range(start_page, end_page + 1):
        page = reader.pages[page_num]
        text += page.extract_text()

    return text

# Extract text from the PDF
pdf_text = extract_text_from_pdf(r"C:\Users\s0Csrak\Downloads\Estados_financieros_(PDF)76265736_202406.pdf",start_page=3, end_page=9)
tokens = encoding.encode(pdf_text)

# Get the number of tokens
num_tokens = len(tokens)
print(f"Number of tokens: {num_tokens}")


def extract_financial_data(text):
    completion = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": "You are an assistant who extracts the following financial data. Focus in: revenue, net profit, operating profit, non controlling profit, eps, operating eps, interest revenue, cash from sales, cash from yield, cash from rent, cash to payments, cash to other payments, speculation cash, current payables, cost of sales, ebit, depreciation, interest, cash, current assets, liabilities, marketable securities, current other assets, provisions for employees, non current assets, goodwill, intangible assets, assets, current liabilities, equity, shares, inventories, shares authorized, net operating cashflows, net investing cashflows, net financing cashflows, payment for supplies, payment to employees, dividends paid, forex, trade receivables, prepayments, cash on hands, cash on banks, cash short investment, employee benefits"},
            {"role": "user",
             "content": f"Please extract the financial data for the current quarter. Express in $, knowing $M is 1000$. Write each in one line separating the name of what was extracted and the value with 3 spaces The text is: {text}"
             }
        ]
    )
    return completion.choices[0].message.content
# Get extracted financial data
financial_data = extract_financial_data(pdf_text)
print(financial_data)
import tiktoken
from openai import OpenAI
# Load the tokenizer for the specific model you plan to use
encoding = tiktoken.encoding_for_model("gpt-4o")
os.environ["OPENAI_API_KEY"] = 'KEY'

client = OpenAI()
def extract_financial_data(pdf_f):
    pdf_text = extract_text_from_pdf(pdf_f, start_page=3, end_page=9)
    tokens = encoding.encode(pdf_text)
    num_tokens = len(tokens)
    print(f"Number of tokens in extracted text: {num_tokens}")
    client = OpenAI()
    completion = client.chat_completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system",
             "content": "You are an assistant who extracts the following financial data. Focus on: revenue, net profit, operating profit, non controlling profit, eps, operating eps, interest revenue, cash from sales, cash from yield, cash from rent, cash to payments, cash to other payments, speculation cash, current payables, cost of sales, ebit, depreciation, interest, cash, current assets, liabilities, marketable securities, current other assets, provisions for employees, non current assets, goodwill, intangible assets, assets, current liabilities, equity, shares, inventories, shares authorized, net operating cashflows, net investing cashflows, net financing cashflows, payment for supplies, payment to employees, dividends paid, forex, trade receivables, prepayments, cash on hands, cash on banks, cash short investment, employee benefits"},
            {"role": "user",
             "content": f"Please extract the financial data for the current quarter. Express in $, knowing $M is 1000$ and $MM is 1000000$. Write each in one line separating the name of what was extracted and the value with 3 spaces. The text is: {pdf_text}"}
        ]
    )
    result = completion.choices[0].message.content
    print(f"Extracted financial data from AI: {result[:200]}...")  # Print first 200 characters for context
    return result
    return 'Cash from sales   131,901,952   \nCash to payments   31,307,163   \nCash to other payments   28,399,362   \nProfit   58,093,668   \nNet profit   58,093,668   \nOperating profit   74,689,031   \nNon controlling profit  58    \nEPS   177.11   \nOperating EPS  168.56   \nInterest revenue  303,746   \nCash from rent  0   \nCash from yield  0    \nCash from sales   131,901,952   \nCash to payments   31,307,163   \nCash to other payments   28,399,362   \nProfit   58,093,668   \nNet profit   58,093,668   \nOperating profit   74,689,031   \nNon controlling profit  58    \nEPS   177.11   \nOperating EPS  168.56   \nInterest revenue  303,746   \nCash from rent  0   \nCash from yield  0    \nCash from sales   131,901,952   \nCash to payments   31,307,163   \nCash to other payments   28,399,362   \nProfit   58,093,668   \nNet profit   58,093,668   \nOperating profit   74,689,031   \nNon controlling profit  58    \nEPS   177.11   \nOperating EPS  168.56  \nInterest revenue  303,746  '

