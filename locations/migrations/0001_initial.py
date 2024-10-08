# Generated by Django 4.1.2 on 2024-09-03 06:24

from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Locations',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('created', models.DateTimeField(auto_now_add=True)),
                ('modified', models.DateTimeField(auto_now=True)),
                ('is_deleted', models.BooleanField(default=False)),
                ('city', models.CharField(max_length=50)),
                ('country', models.CharField(max_length=50)),
                ('country_code_iso2', models.CharField(max_length=2)),
                ('country_code_iso3', models.CharField(max_length=3)),
                ('state', models.CharField(max_length=50)),
                ('state_code', models.CharField(blank=True, max_length=2, null=True)),
            ],
        ),
        migrations.AddIndex(
            model_name='locations',
            index=models.Index(fields=['city', 'country', 'state'], name='locations_l_city_ef6c53_idx'),
        ),
        migrations.AlterUniqueTogether(
            name='locations',
            unique_together={('city', 'country', 'state')},
        ),
    ]
