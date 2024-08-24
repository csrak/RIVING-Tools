from django.http import JsonResponse
from django.shortcuts import render
from .models import FinancialData, FinancialRatio
from django.db.models import Max, Subquery, OuterRef, Q

def complex_analysis(request):
    return render(request, 'complex_analysis.html')

def filter_ratios(request):
    ratio_names = request.GET.getlist('ratio_names[]')  # Get the list of ratios
    filters = request.GET.getlist('filters[]')  # List of operators and values

    # List of valid ratio names
    valid_ratios = [
        'pe_ratio', 'pb_ratio', 'ps_ratio', 'peg_ratio', 'ev_ebitda',
        'gross_profit_margin', 'operating_profit_margin', 'net_profit_margin',
        'return_on_assets', 'return_on_equity', 'debt_to_equity', 'current_ratio', 'quick_ratio'
    ]

    # Ensure only valid ratios are processed
    ratio_names = [r for r in ratio_names if r in valid_ratios]

    if not ratio_names:
        return JsonResponse({'error': 'No valid ratios selected'}, status=400)

    # Build the filter condition
    q_filters = Q()
    for i, ratio_name in enumerate(ratio_names):
        operator, value = filters[i].split(':')
        if operator not in ['gt', 'lt', 'gte', 'lte']:
            return JsonResponse({'error': f'Invalid operator for {ratio_name}: {operator}'}, status=400)
        q_filters &= Q(**{f'{ratio_name}__{operator}': float(value)})

    # Subquery to find the latest date for each ticker
    latest_dates_subquery = FinancialRatio.objects.filter(
        ticker=OuterRef('ticker')
    ).order_by('-date').values('date')[:1]

    # Filter ratios based on the latest date per ticker and the built conditions
    filtered_ratios = FinancialRatio.objects.filter(
        date=Subquery(latest_dates_subquery)
    ).filter(q_filters).values('ticker', 'date', *ratio_names).order_by('ticker')

    # Ensure that all selected ratios are returned in the response
    data = list(filtered_ratios)

    # For each row, make sure all selected ratio columns are included
    for row in data:
        for ratio in ratio_names:
            if ratio not in row:
                row[ratio] = None  # or 'N/A', or any other placeholder

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
