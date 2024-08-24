# Generated by Django 4.1 on 2024-08-23 18:05

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("fin_data_cl", "0003_remove_financialratio_debt_to_equity_ratio_and_more"),
    ]

    operations = [
        migrations.AlterModelOptions(name="pricedata", options={},),
        migrations.AlterUniqueTogether(name="pricedata", unique_together=set(),),
        migrations.AddField(
            model_name="pricedata",
            name="market_cap",
            field=models.DecimalField(
                blank=True, decimal_places=2, max_digits=30, null=True
            ),
        ),
    ]
