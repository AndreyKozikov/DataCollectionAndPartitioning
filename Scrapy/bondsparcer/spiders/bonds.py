import scrapy
from scrapy.http import HtmlResponse
from bondsparcer.items import BondsparcerItem


class BondsSpider(scrapy.Spider):
    name = "bonds"
    allowed_domains = ["blackterminal.com"]
    start_urls = ["https://blackterminal.com/bonds?page=1"]
    base_url = "https://blackterminal.com"

    def parse(self, response: HtmlResponse):
        next_page = response.xpath("//li[@class='page-item next']/a/@href").get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)

        yield from self.bonds_parce(response)


    def bonds_parce(self, response: HtmlResponse):
        # Извлечение данных из таблицы
        rows = response.xpath("//div[@id='w7-container']/table/tbody/tr")
        for row in rows:
            item = BondsparcerItem(
                name=row.xpath(".//td[1]//a/text()").get(),
                nominal=row.xpath(".//td[2]/text()").get(),
                price=row.xpath(".//td[3]/text()").get(),
                nkd=row.xpath(".//td[4]/text()").get(),
                cost=row.xpath(".//td[5]/text()").get(),
                coupon=row.xpath(".//td[6]/text()").get(),
                rate=row.xpath(".//td[7]/text()").get(),
                profitability=row.xpath(".//td[8]/text()").get(),
            )
            yield item
