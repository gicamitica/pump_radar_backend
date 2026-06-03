import { CheerioCrawler } from 'crawlee';

const results = [];

const crawler = new CheerioCrawler({
    maxRequestsPerCrawl: 3,
    async requestHandler({ $, request }) {
        const posts = [];
        $('div[data-testid="post-container"]').each((i, el) => {
            const title = $(el).find('h3').text().trim();
            if (title) posts.push(title);
        });
        console.log(`URL: ${request.url}, Posts found: ${posts.length}`);
        posts.slice(0, 3).forEach(p => console.log(' -', p));
    },
});

await crawler.run(['https://old.reddit.com/r/CryptoMoonShots/new/.json?limit=5']);
