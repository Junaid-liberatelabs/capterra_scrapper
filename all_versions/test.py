from firecrawl import FirecrawlApp

app = FirecrawlApp(api_key="fc-c8a7ebbdff494023aa0cd9cb0bad932b")

# Scrape a website:
scrape_result = app.scrape_url('https://www.capterra.com/p/213339/Google-Ads/reviews/', 
    formats=[ 'markdown'], 
    actions=[
       
        {
            "type": "click",
            "selector": "show-more-reviews"},
         {"type": "wait", "milliseconds":   2000},
           {
            "type": "click",
            "selector": "show-more-reviews"},
              {"type": "wait", "milliseconds":   200},
       
       
          {
            "type": "click",
            "selector": "show-more-reviews"},
          {"type": "wait", "milliseconds":   200},
       
          {
            "type": "click",
            "selector": "show-more-reviews"},
          {"type": "wait", "milliseconds":   200},
       
          {
            "type": "click",
            "selector": "show-more-reviews"},
          {"type": "wait", "milliseconds":   200},
       
          {
            "type": "click",
            "selector": "show-more-reviews"},
          {"type": "wait", "milliseconds":   200},
      
       
      
       
          {
            "type": "click",
            "selector": "show-more-reviews"},
          {"type": "wait", "milliseconds":   200},
       
          {
            "type": "click",
            "selector": "show-more-reviews"},
          {"type": "wait", "milliseconds":   200},
       
          {
            "type": "click",
            "selector": "show-more-reviews"},
          {"type": "wait", "milliseconds":   200},
       
          {
            "type": "click",
            "selector": "show-more-reviews"},
          {"type": "wait", "milliseconds":   200},
       
          
       
       
          {
            "type": "click",
            "selector": "show-more-reviews"},
          {"type": "wait", "milliseconds":   200},
       
          {
            "type": "click",
            "selector": "show-more-reviews"},
          {"type": "wait", "milliseconds":   200},
       
          {
            "type": "click",
            "selector": "show-more-reviews"},
          {"type": "wait", "milliseconds":   200},
       
          {
            "type": "click",
            "selector": "show-more-reviews"},
          {"type": "wait", "milliseconds":   200},
       
          {
            "type": "click",
            "selector": "show-more-reviews"},
          {"type": "wait", "milliseconds":   200},
       
          {
            "type": "click",
            "selector": "show-more-reviews"},
          {"type": "wait", "milliseconds":   200},
       
          {
            "type": "click",
            "selector": "show-more-reviews"},
          {"type": "wait", "milliseconds":   200},
       
          {
            "type": "click",
            "selector": "show-more-reviews"},
          {"type": "wait", "milliseconds":   200},
       
          {
            "type": "click",
            "selector": "show-more-reviews"},
          {"type": "wait", "milliseconds":   200},
       

    
          
    ]
    
)
with open('output.md', 'w', encoding='utf-8') as f:
    f.write(scrape_result.markdown)
    