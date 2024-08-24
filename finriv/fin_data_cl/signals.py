# from django_q.tasks import async_task, schedule, Schedule
# from datetime import datetime, timedelta
# from django.utils import timezone  # Import timezone utility
# from fin_data_cl.tasks import update_prices_and_calculate_ratios
#
# def schedule_tasks(sender, **kwargs):
#     # Schedule task if not already scheduled
#     if not Schedule.objects.filter(func='fin_data_cl.tasks.update_prices_and_calculate_ratios').exists():
#         schedule(
#             'fin_data_cl.tasks.update_prices_and_calculate_ratios',
#             schedule_type=Schedule.DAILY,
#             repeats=-1,  # Run indefinitely
#             next_run=timezone.now() + timedelta(days=1)  # Use timezone-aware datetime
#         )