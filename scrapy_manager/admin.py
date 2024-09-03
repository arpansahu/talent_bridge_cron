from django.contrib import admin
from .models import ScrapyProject, ScrapySpider, ScrapyJob
from .tasks import run_spider

class ScrapySpiderAdmin(admin.ModelAdmin):
    list_display = ('name', 'project')
    actions = ['trigger_spider']

    def trigger_spider(self, request, queryset):
        for spider in queryset:
            run_spider.delay(spider.id)
            self.message_user(request, f'Spider {spider.name} has been scheduled to run.')

    trigger_spider.short_description = "Run selected spiders"

admin.site.register(ScrapyProject)
admin.site.register(ScrapySpider, ScrapySpiderAdmin)
admin.site.register(ScrapyJob)