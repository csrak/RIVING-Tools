from django.http import JsonResponse
from django.shortcuts import render
from .models import FinancialData, FinancialRatio, PriceData
from django.db.models import Q, Subquery, OuterRef, DecimalField, F
from django.db.models.functions import Cast


def complex_analysis(request):
    return render(request, 'complex_analysis.html')

def filter_ratios(request):
    filters = request.GET.getlist('filters[]')

    # Define valid ratios
    valid_ratios = [
        'pe_ratio', 'pb_ratio', 'ps_ratio', 'peg_ratio', 'ev_ebitda',
        'gross_profit_margin', 'operating_profit_margin', 'net_profit_margin',
        'return_on_assets', 'return_on_equity', 'debt_to_equity', 'current_ratio', 'quick_ratio'
    ]

    q_filters = Q()

    # Apply each filter
    for filter_string in filters:
        ratio_name, operator, value = filter_string.split(':')
        if ratio_name in valid_ratios and operator in ['gt', 'lt', 'gte', 'lte']:
            filter_expr = f'{ratio_name}__{operator}'
            q_filters &= Q(**{filter_expr: float(value)})

    # Subquery to find the latest date for each ticker in FinancialRatio
    latest_date_subquery = FinancialRatio.objects.filter(
        ticker=OuterRef('ticker')
    ).order_by('-date').values('date')[:1]

    # Apply filters to get the latest financial ratios
    filtered_ratios = FinancialRatio.objects.filter(
        date=Subquery(latest_date_subquery)
    ).filter(q_filters).annotate(
        latest_price=Cast(
            Subquery(
                PriceData.objects.filter(
                    ticker=OuterRef('ticker')
                ).order_by('-date').values('price')[:1]
            ), DecimalField(max_digits=20, decimal_places=2)
        ),
        latest_market_cap=Cast(
            Subquery(
                PriceData.objects.filter(
                    ticker=OuterRef('ticker')
                ).order_by('-date').values('market_cap')[:1]
            ), DecimalField(max_digits=30, decimal_places=2)
        )
    ).values('ticker', 'date', *valid_ratios, 'latest_price', 'latest_market_cap').order_by('ticker')

    data = list(filtered_ratios)

    return JsonResponse(data, safe=False)

def get_metrics(request, ticker, metric_name):
    if metric_name not in [field.name for field in FinancialData._meta.get_fields() if field.name not in ['id', 'date', 'ticker']]:
        return JsonResponse({'error': 'Invalid metric name'}, status=400)

    data = FinancialData.objects.filter(ticker=ticker).values('date', metric_name)
    return JsonResponse(list(data), safe=False)

def index(request):
    tickers = FinancialData.objects.values_list('ticker', flat=True).distinct()
    metrics = [field.name for field in FinancialData._meta.get_fields() if field.name not in ['id', 'date', 'ticker']]
    return render(request, 'index.html', {'tickers': tickers, 'metrics': metrics})

def get_data(request):
    ticker = request.GET.get('ticker')
    metrics = request.GET.getlist('metric[]')

    # Ensure 'date' is always included
    fields_to_fetch = ['date'] + metrics

    # Query the database for the selected metrics
    data = FinancialData.objects.filter(ticker=ticker).order_by('date').values(*fields_to_fetch)

    # Convert QuerySet to a list to return as JSON
    data_list = list(data)

    return JsonResponse(data_list, safe=False)

def ticker_suggestions(request):
    query = request.GET.get('query', '')
    suggestions = FinancialData.objects.filter(ticker__icontains=query).values_list('ticker', flat=True).distinct()
    return JsonResponse(list(suggestions), safe=False)

def about(request):
    return render(request, 'about.html')

def contact(request):
    return render(request, 'contact.html')
