# my_app/management/commands/run_my_script.py
from django.core.management.base import BaseCommand
#from finriv.utils.scrapping_classes import test as test_ticker
from finriv.settings import BASE_DIR
from finriv.utils.scrapping_classes import Ticker,TestCmfScraping
from django.fin_data_cl.models import FinancialReport
import unittest
from pathlib import Path
G_datafold = BASE_DIR / 'finriv' / 'Data' / 'Chile'
G_root_dir = BASE_DIR

import os
import pandas as pd
from pathlib import Path
import fitz  # PyMuPDF
import re
import json
from llama_index.llms.ollama import Ollama

from dataclasses import dataclass
from typing import List, Optional
from enum import Enum
from llama_index.core import Settings, VectorStoreIndex, Document
from llama_index.core.node_parser import SimpleNodeParser
from llama_index.core.retrievers import VectorIndexRetriever
from llama_index.core.query_engine import RetrieverQueryEngine
from llama_index.core.prompts import PromptTemplate
from llama_index.core.llms import ChatMessage
from llama_index.llms.openai import OpenAI

def get_api_key(file_path):
    with open(file_path, 'r') as file:
        api_key = file.read().strip()  # Strip removes any extra spaces or newlines
    return api_key

class RiskCategory(Enum):
    OPERATIONAL = "Operational"
    FINANCIAL = "Financial"
    MARKET = "Market"
    REGULATORY = "Regulatory"


class Likelihood(Enum):
    HIGH = "High"
    MEDIUM = "Medium"
    LOW = "Low"


@dataclass
class Risk:
    category: RiskCategory
    description: str
    potential_impact: str


@dataclass
class FinancialMetrics:
    revenue: Optional[float] = None
    net_income: Optional[float] = None
    operating_margin: Optional[float] = None
    debt_to_equity: Optional[float] = None


@dataclass
class HistoricalChange:
    category: str
    description: str
    impact: str


@dataclass
class FutureOutlook:
    category: str
    description: str
    likelihood: Likelihood


@dataclass
class FinancialAnalysis:
    business_overview: str
    risks: List[Risk]
    metrics: FinancialMetrics
    historical_changes: List[HistoricalChange]
    future_outlook: List[FutureOutlook]


class FinancialDocumentAnalyzer:
    def __init__(self, api_key: str, model: str = "gpt-4"):
        # Initialize LlamaIndex settings
        self.llm = OpenAI(model=model, api_key=api_key)
        Settings.llm = self.llm
        self.node_parser = SimpleNodeParser.from_defaults()

        # Define query templates
        self._init_query_templates()

    def _init_query_templates(self):
        """Initialize query templates for different analysis components"""
        self.queries = {
            "business_overview": """
                Extract a concise business overview from the financial document.
                Focus on: main business activities, revenue streams, and market position.
                Be specific and include key details about the company's operations, including countries and regions.
            """,

            "risks": """
                List the main risks mentioned in the financial document.
                For each risk, specify:
                1. Risk Category (must be one of: Operational, Financial, Market, Regulatory)
                2. Detailed description of the risk
                3. Potential impact on the business
                Format each risk on a new line starting with the category.
            """,

            "metrics": """
                Extract the following financial metrics from the document:
                - Revenue (in millions/billions)
                - Net Income (in millions/billions)
                - Operating Margin (as percentage)
                - Debt to Equity Ratio
                Provide only the numbers, with each metric on a new line.
            """,

            "changes": """
                Identify significant changes mentioned in the document compared to previous periods.
                For each change:
                1. Specify the category of change
                2. Describe what changed
                3. Explain the impact
                List each change on a new line.
            """,

            "outlook": """
                Extract future outlook information from the document.
                For each point:
                1. Specify the category
                2. Describe the expected change or development
                3. Indicate likelihood as: High, Medium, or Low
                List each point on a new line.
            """
        }

    def _parse_risks_response(self, response: str) -> List[Risk]:
        risks = []
        lines = [line.strip() for line in response.split('\n') if line.strip()]

        for line in lines:
            try:
                # Expected format: "Category: Description (Impact: impact_text)"
                parts = line.split(':', 2)
                if len(parts) >= 2:
                    category_str = parts[0].strip().upper()
                    remaining = parts[1].strip()

                    # Split description and impact
                    if '(Impact:' in remaining:
                        desc_part, impact_part = remaining.split('(Impact:', 1)
                        description = desc_part.strip()
                        impact = impact_part.replace(')', '').strip()
                    else:
                        description = remaining
                        impact = "Not specified"

                    # Map category string to enum
                    try:
                        category = RiskCategory[category_str]
                    except KeyError:
                        category = RiskCategory.OPERATIONAL  # Default category

                    risks.append(Risk(
                        category=category,
                        description=description,
                        potential_impact=impact
                    ))
            except Exception as e:
                print(f"Error parsing risk line: {line}, Error: {str(e)}")
                continue

        return risks

    def _parse_metrics_response(self, response: str) -> FinancialMetrics:
        metrics = FinancialMetrics()
        lines = [line.strip() for line in response.split('\n') if line.strip()]

        for line in lines:
            try:
                if ':' in line:
                    key, value = line.split(':', 1)
                    key = key.strip().lower()
                    value = value.strip()

                    # Remove any currency symbols and convert to float
                    value = ''.join(c for c in value if c.isdigit() or c in '.-')
                    try:
                        float_value = float(value)
                    except ValueError:
                        continue

                    if 'revenue' in key:
                        metrics.revenue = float_value
                    elif 'net income' in key:
                        metrics.net_income = float_value
                    elif 'operating margin' in key:
                        metrics.operating_margin = float_value
                    elif 'debt to equity' in key:
                        metrics.debt_to_equity = float_value
            except Exception as e:
                print(f"Error parsing metric line: {line}, Error: {str(e)}")
                continue

        return metrics

    def _parse_changes_response(self, response: str) -> List[HistoricalChange]:
        changes = []
        lines = [line.strip() for line in response.split('\n') if line.strip()]

        for line in lines:
            try:
                if ':' in line:
                    category, rest = line.split(':', 1)
                    # Look for impact indication
                    if '(Impact:' in rest:
                        description, impact = rest.split('(Impact:', 1)
                        impact = impact.replace(')', '').strip()
                    else:
                        description = rest
                        impact = "Not specified"

                    changes.append(HistoricalChange(
                        category=category.strip(),
                        description=description.strip(),
                        impact=impact
                    ))
            except Exception as e:
                print(f"Error parsing change line: {line}, Error: {str(e)}")
                continue

        return changes

    def _parse_outlook_response(self, response: str) -> List[FutureOutlook]:
        outlook_items = []
        lines = [line.strip() for line in response.split('\n') if line.strip()]

        for line in lines:
            try:
                if ':' in line:
                    category, rest = line.split(':', 1)
                    # Look for likelihood indication
                    if '(' in rest and ')' in rest:
                        description_part, likelihood_part = rest.rsplit('(', 1)
                        likelihood_str = likelihood_part.replace(')', '').strip().upper()
                        try:
                            likelihood = Likelihood[likelihood_str]
                        except KeyError:
                            likelihood = Likelihood.MEDIUM
                    else:
                        description_part = rest
                        likelihood = Likelihood.MEDIUM

                    outlook_items.append(FutureOutlook(
                        category=category.strip(),
                        description=description_part.strip(),
                        likelihood=likelihood
                    ))
            except Exception as e:
                print(f"Error parsing outlook line: {line}, Error: {str(e)}")
                continue

        return outlook_items

    def analyze_document(self, document_text: str) -> Optional[FinancialAnalysis]:
        try:
            # Create document and index
            document = Document(text=document_text)
            nodes = self.node_parser.get_nodes_from_documents([document])
            index = VectorStoreIndex(nodes)

            # Create query engine
            query_engine = index.as_query_engine()

            # Execute queries and parse responses
            responses = {}
            for query_type, query_text in self.queries.items():
                response = query_engine.query(query_text)
                responses[query_type] = response.response

            # Parse responses into structured data
            analysis = FinancialAnalysis(
                business_overview=responses["business_overview"].strip(),
                risks=self._parse_risks_response(responses["risks"]),
                metrics=self._parse_metrics_response(responses["metrics"]),
                historical_changes=self._parse_changes_response(responses["changes"]),
                future_outlook=self._parse_outlook_response(responses["outlook"])
            )

            return analysis

        except Exception as e:
            print(f"Error analyzing financial document: {str(e)}")
            return None

    def get_summary(self, analysis: FinancialAnalysis) -> str:
        """Generate a human-readable summary from the analysis"""
        summary = f"""
Financial Analysis Summary

Business Overview:
{analysis.business_overview}

Key Risks:
{self._format_risks(analysis.risks)}

Financial Metrics:
{self._format_metrics(analysis.metrics)}

Historical Changes:
{self._format_changes(analysis.historical_changes)}

Future Outlook:
{self._format_outlook(analysis.future_outlook)}
"""
        return summary

    def _format_risks(self, risks: List[Risk]) -> str:
        return "\n".join([
            f"- {risk.category.value}: {risk.description} (Impact: {risk.potential_impact})"
            for risk in risks
        ])

    def _format_metrics(self, metrics: FinancialMetrics) -> str:
        formatted = []
        if metrics.revenue is not None:
            formatted.append(f"- Revenue: {metrics.revenue:,.2f}")
        if metrics.net_income is not None:
            formatted.append(f"- Net Income: {metrics.net_income:,.2f}")
        if metrics.operating_margin is not None:
            formatted.append(f"- Operating Margin: {metrics.operating_margin:.2f}%")
        if metrics.debt_to_equity is not None:
            formatted.append(f"- Debt to Equity: {metrics.debt_to_equity:.2f}")
        return "\n".join(formatted)

    def _format_changes(self, changes: List[HistoricalChange]) -> str:
        return "\n".join([
            f"- {change.category}: {change.description} (Impact: {change.impact})"
            for change in changes
        ])

    def _format_outlook(self, outlook: List[FutureOutlook]) -> str:
        return "\n".join([
            f"- {item.category} ({item.likelihood.value}): {item.description}"
            for item in outlook
        ])

class FileSearcher:
    def __init__(self, datafold_path, use_llamaindex=True):
        self.datafold_path = Path(datafold_path)
        self.all_tickers = Ticker.get_all_tickers_as_dataframe()
        self.use_llamaindex = use_llamaindex
        self.results_df = pd.DataFrame(columns=['Ticker', 'Year', 'Month', 'Response'])

    def load_tickers(self):
        # Get all tickers as dataframe
        self.all_tickers = Ticker.get_all_tickers_as_dataframe()

    def search_files_for_tickers(self):
        if self.all_tickers is None:
            raise ValueError("Tickers dataframe is not loaded. Call load_tickers() first.")

        # Iterate through each ticker
        for _, row in self.all_tickers.iterrows():
            ticker = row['Ticker']

            # Iterate through each folder in datafold_path
            for folder in self.datafold_path.iterdir():
                if folder.is_dir() and '-' in folder.name:  # Assuming folders are named as 'MM-YYYY'
                    # Extract month and year from folder name
                    month, year = folder.name.split('-')

                    # Construct expected filename
                    file_name = f"Analisis_{ticker}_{folder.name}.pdf"
                    file_path = folder / file_name

                    # Check if the file exists
                    if file_path.exists():
                        print(f"File found: {file_path}")
                        response = self.parse_pdf(file_path)
                        if response:
                            self.save_response(ticker, year, month, response)
                    else:
                        print(f"File not found: {file_name} in {folder}")

    def parse_pdf(self, file_path):
        try:
            with fitz.open(file_path) as pdf_file:
                text_content = ""

                # Extract text from each page
                for page_num in range(pdf_file.page_count):
                    page = pdf_file.load_page(page_num)
                    text_content += page.get_text("text") + "\n"

                # Pre-process text for LLM
                preprocessed_text = self.preprocess_text(text_content)

                # Choose parsing method
                if self.use_llamaindex:
                    analyzer = FinancialDocumentAnalyzer(api_key=get_api_key(G_root_dir))

                    # Analyze document
                    analysis = analyzer.analyze_document(preprocessed_text)

                    if analysis:
                        # Access structured data
                        print(f"Business Overview: {analysis.business_overview}")
                        for risk in analysis.risks:
                            print(f"Risk: {risk.category.value} - {risk.description}")

                        # Get formatted summary
                        summary = analyzer.get_summary(analysis)
                        print(summary)
                    exit()
                else:
                    return self.query_ollama(preprocessed_text)
        except Exception as e:
            print(f"Failed to parse PDF {file_path}: {e}")
            return None

    def preprocess_text(self, text):
        # Remove extra whitespace and line breaks
        text = re.sub(r'\s+', ' ', text).strip()

        # Split into smaller chunks for LLM (e.g., 1000-character chunks)
        chunk_size = 1000
        chunks = [text[i:i + chunk_size] for i in range(0, len(text), chunk_size)]

        # Convert to JSON structure
        structured_data = {"chunks": chunks}
        return json.dumps(structured_data, indent=2)

    def save_response(self, ticker, year, month, analysis):
        # Save the response to the CSV file for debugging
        new_row = {
            'Ticker': ticker,
            'Year': year,
            'Month': month,
            'Response': self._format_response_as_string(analysis)
        }
        self.results_df = pd.concat([self.results_df, pd.DataFrame([new_row])], ignore_index=True)
        print(f"Saved response for ticker {ticker}, year {year}, and month {month}")

        # Save to Django model
        financial_report, created = FinancialReport.objects.get_or_create(
            ticker=ticker,
            year=year,
            month=month,
            defaults={
                'business_overview': analysis.business_overview,
                'risks': [risk.__dict__ for risk in analysis.risks],
                'metrics': analysis.metrics.__dict__,
                'historical_changes': [change.__dict__ for change in analysis.historical_changes],
                'future_outlook': [outlook.__dict__ for outlook in analysis.future_outlook],
            }
        )

        if not created:
            # Update the existing object if it already exists
            financial_report.business_overview = analysis.business_overview
            financial_report.risks = [risk.__dict__ for risk in analysis.risks]
            financial_report.metrics = analysis.metrics.__dict__
            financial_report.historical_changes = [change.__dict__ for change in analysis.historical_changes]
            financial_report.future_outlook = [outlook.__dict__ for outlook in analysis.future_outlook]
            financial_report.save()

    def _format_response_as_string(self, analysis):
        """ Helper method to format the analysis as a string for CSV saving. """
        return f"""
Business Overview: {analysis.business_overview}
Risks: {self._format_risks(analysis.risks)}
Metrics: {self._format_metrics(analysis.metrics)}
Historical Changes: {self._format_changes(analysis.historical_changes)}
Future Outlook: {self._format_outlook(analysis.future_outlook)}
"""

    def query_llamaindex(self, preprocessed_text):
        # Use OpenAI's GPT model to query the document
        try:
            llm = OpenAI(model="gpt-4o-mini", api_key=get_api_key(G_root_dir))

            # Formulate the query
            messages = [
                ChatMessage(role="system", content="You are an expert financial document analyst. You give detailed and long answers and focus on finding unusual information."),
                ChatMessage(role="user", content=(
                    "Please parse the following document and determine the following: \n"
                    "1. Main Risks listed by the company. \n"
                    "2. Main business of the company. \n"
                    "3. Main key information. \n"
                    "4. Main key changes from last year. \n"
                    "5. Main key changes expected in the future. \n"
                    f"Document content: {preprocessed_text}"
                ))
            ]

            response = llm.chat(messages)
            return response
        except Exception as e:
            print(f"Failed to query LlamaIndex (OpenAI GPT-4): {e}")
            return None

    def query_ollama(self, preprocessed_text):
        # Use Ollama Llama3 to query the document
        try:
            llm = Ollama(model="llama3:latest", request_timeout=360.0)

            # Formulate the query
            messages = [
                ChatMessage(role="system", content="You are an expert financial document analyst."),
                ChatMessage(role="user", content=(
                    "Please parse the following document and determine the following: \n"
                    "1. Main Risks listed by the company. \n"
                    "2. Main business of the company. \n"
                    "3. Main key information. \n"
                    "4. Main key changes from last year. \n"
                    "5. Main key changes expected in the future. \n"
                    f"Document content: {preprocessed_text}"
                ))
            ]

            response = llm.chat(messages)
            return response
        except Exception as e:
            print(f"Failed to query Ollama (Llama3): {e}")
            return None
class Command(BaseCommand):
    help = 'Run the utility script'

    def handle(self, *args, **kwargs):
        self.stdout.write('Running utility script...')
        #test_ticker()
        #suite = unittest.TestLoader().loadTestsFromTestCase(TestCmfScraping)

        # Run the test suite
        #unittest.TextTestRunner(verbosity=2).run(suite)
        searcher = FileSearcher(G_datafold, use_llamaindex=True)
        searcher.search_files_for_tickers()
        # Optionally save the results to a CSV file
        output_file = G_datafold/"results.csv"
        searcher.results_df.to_csv(output_file, index=False)
        print(f"Results saved to {output_file}")
        self.stdout.write(self.style.SUCCESS('Script ran successfully!'))

