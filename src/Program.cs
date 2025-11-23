using Microsoft.Data.Sqlite;
using HtmlAgilityPack;

namespace UrlCrawler.Net
{
    class Program
    {
        private static readonly HttpClient httpClient = new HttpClient();
        private static string connectionString = "Data Source=rulcrawler.sqlite";

        static async Task Main(string[] args)
        {
            Console.Clear();
            Console.WriteLine("Web Crawler Started!");
            
            // Initialize database
            InitializeDatabase();
            
            Console.Write("Enter website URL to crawl: ");
            string startUrl = Console.ReadLine() ?? "";
            if(!startUrl.Contains("http"))
                startUrl = "https://" + startUrl;
            
            Console.Write("Enter maximum pages to crawl: ");
            int maxPages = int.Parse(Console.ReadLine() ?? "0");

            await StartCrawling(startUrl, maxPages);
            
            Console.WriteLine("Crawling completed!");
        }

        static void InitializeDatabase()
        {
            using var connection = new SqliteConnection(connectionString);
            connection.Open();
            
            // Create tables
            var commands = new[]
            {
                @"CREATE TABLE IF NOT EXISTS Page (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Url TEXT NOT NULL,
                    Title TEXT,
                    StatusCode INTEGER,
                    ContentType TEXT,
                    LastModified DATETIME,
                    CrawledDate DATETIME DEFAULT CURRENT_TIMESTAMP
                )",
                
                @"CREATE TABLE IF NOT EXISTS Content (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    PageId INTEGER NOT NULL,
                    TagType TEXT NOT NULL,
                    Sequence TEXT NOT NULL,
                    Level INTEGER NOT NULL,
                    TextContent TEXT NOT NULL,
                    FOREIGN KEY (PageId) REFERENCES Page(Id)
                )"
            };
            
            foreach (var commandText in commands)
            {
                using var command = new SqliteCommand(commandText, connection);
                command.ExecuteNonQuery();
            }
        }

        static async Task StartCrawling(string startUrl, int maxPages)
        {
            var queue = new Queue<CrawlItem>();
            var visited = new HashSet<string>();
            
            queue.Enqueue(new CrawlItem { Url = startUrl, Depth = 0 });
            visited.Add(NormalizeUrl(startUrl));

            int crawledCount = 0;

            while (queue.Count > 0 && crawledCount < maxPages)
            {
                var currentItem = queue.Dequeue();
                
                try
                {
                    Console.WriteLine($"Crawling: {currentItem.Url}");
                    
                    var pageData = await CrawlPage(currentItem.Url ?? "");
                    
                    if (pageData != null)
                    {
                        await SavePageAndContentToDatabase(pageData);
                        crawledCount++;
                        
                        // Extract and queue new URLs
                        if (currentItem.Depth < 2)
                        {
                            var newUrls = ExtractUrls(pageData.HtmlContent ?? "", currentItem.Url ?? "");
                            foreach (var url in newUrls)
                            {
                                var normalizedUrl = NormalizeUrl(url);
                                if (!visited.Contains(normalizedUrl))
                                {
                                    visited.Add(normalizedUrl);
                                    queue.Enqueue(new CrawlItem { Url = url, Depth = currentItem.Depth + 1 });
                                }
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error crawling {currentItem.Url}: {ex.Message}");
                }
                
                // Be considerate, give some time to relax
                await Task.Delay(2000);
            }
        }

        static async Task<PageData?> CrawlPage(string url)
        {
            try
            {
                httpClient.DefaultRequestHeaders.UserAgent.ParseAdd(
                    "Mozilla/5.0 (compatible; WebCrawler/1.0)");
                
                var response = await httpClient.GetAsync(url);
                
                if (!response.IsSuccessStatusCode)
                    return null;

                var content = await response.Content.ReadAsStringAsync();
                var contentType = response.Content.Headers.ContentType?.MediaType ?? "";

                if (!contentType.Contains("text/html"))
                    return null;

                var htmlDoc = new HtmlDocument();
                htmlDoc.LoadHtml(content);

                var title = htmlDoc.DocumentNode.SelectSingleNode("//title")?.InnerText?.Trim();

                return new PageData
                {
                    Url = url,
                    Title = title,
                    HtmlContent = content,
                    StatusCode = (int)response.StatusCode,
                    ContentType = contentType,
                    LastModified = response.Content.Headers.LastModified?.DateTime,
                    CrawledDate = DateTime.UtcNow
                };
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error fetching {url}: {ex.Message}");
                return null;
            }
        }

        static List<string> ExtractUrls(string htmlContent, string baseUrl)
        {
            var urls = new List<string>();
            var htmlDoc = new HtmlDocument();
            htmlDoc.LoadHtml(htmlContent);

            var linkNodes = htmlDoc.DocumentNode.SelectNodes("//a[@href]");
            if (linkNodes == null) return urls;

            foreach (var node in linkNodes)
            {
                var href = node.GetAttributeValue("href", "");
                if (!string.IsNullOrEmpty(href))
                {
                    var absoluteUrl = GetAbsoluteUrl(baseUrl, href);
                    if (absoluteUrl != null && IsValidUrl(absoluteUrl))
                    {
                        urls.Add(absoluteUrl);
                    }
                }
            }

            return urls;
        }

        static async Task SavePageAndContentToDatabase(PageData pageData)
        {
            using var connection = new SqliteConnection(connectionString);
            await connection.OpenAsync();

            using var transaction = connection.BeginTransaction();

            try
            {
                // Insert into Page table
                var pageSql = @"
                    INSERT INTO Page (Url, Title, StatusCode, ContentType, LastModified, CrawledDate)
                    VALUES (@Url, @Title, @StatusCode, @ContentType, @LastModified, @CrawledDate);
                    SELECT last_insert_rowid();";

                long pageId;
                using (var pageCommand = new SqliteCommand(pageSql, connection, transaction))
                {
                    pageCommand.Parameters.AddWithValue("@Url", pageData.Url);
                    pageCommand.Parameters.AddWithValue("@Title", !String.IsNullOrEmpty(pageData.Title) ? pageData.Title : DBNull.Value);
                    pageCommand.Parameters.AddWithValue("@StatusCode", pageData.StatusCode);
                    pageCommand.Parameters.AddWithValue("@ContentType",  !String.IsNullOrEmpty(pageData.ContentType) ? pageData.ContentType : DBNull.Value);
                    pageCommand.Parameters.AddWithValue("@LastModified", pageData.LastModified != null ? pageData.LastModified : DBNull.Value);
                    pageCommand.Parameters.AddWithValue("@CrawledDate", pageData.CrawledDate);

                    pageId = (long)pageCommand.ExecuteScalar();
                }

                // Parse and save content hierarchy
                await SaveContentHierarchy(connection, transaction, pageId, pageData.HtmlContent ?? "");
                
                transaction.Commit();
                Console.WriteLine($"Saved page and content: {pageData.Url} (Page ID: {pageId})");
            }
            catch (Exception ex)
            {
                transaction.Rollback();
                Console.WriteLine($"Error saving page {pageData.Url}: {ex.Message}");
                throw;
            }
        }

        static async Task SaveContentHierarchy(SqliteConnection connection, SqliteTransaction transaction, long pageId, string htmlContent)
        {
            var htmlDoc = new HtmlDocument();
            htmlDoc.LoadHtml(htmlContent);

            // Remove styling tags but keep their text content
            RemoveStylingTags(htmlDoc);

            // Get body content
            var bodyNode = htmlDoc.DocumentNode.SelectSingleNode("//body");
            if (bodyNode == null) return;

            // Get top-level children of body
            var topLevelNodes = bodyNode.ChildNodes
                .Where(n => !string.IsNullOrWhiteSpace(n.InnerText) && 
                           IsContentTag(n.Name) && 
                           !string.IsNullOrWhiteSpace(n.InnerText.Trim()))
                .ToList();

            int sequenceCounter = 0;

            foreach (var node in topLevelNodes)
            {
                sequenceCounter++;
                await SaveNodeRecursively(connection, transaction, pageId, node, sequenceCounter.ToString(), 0);
            }
        }

        static async Task SaveNodeRecursively(SqliteConnection connection, SqliteTransaction transaction, long pageId, HtmlNode node, string sequence, int level)
        {

            // Get text content (preserve all text, including nested tags' text)
            var textContent = node.InnerText?.Trim();
            if (string.IsNullOrWhiteSpace(textContent))
                return;

            // Insert content
            var contentSql = @"
                INSERT INTO Content (PageId, TagType, Sequence, Level, TextContent)
                VALUES (@PageId, @TagType, @Sequence, @Level, @TextContent)";

            using var contentCommand = new SqliteCommand(contentSql, connection, transaction);
            contentCommand.Parameters.AddWithValue("@PageId", pageId);
            contentCommand.Parameters.AddWithValue("@TagType", node.Name);
            contentCommand.Parameters.AddWithValue("@Sequence", sequence);
            contentCommand.Parameters.AddWithValue("@Level", level);
            contentCommand.Parameters.AddWithValue("@TextContent", textContent);

            await contentCommand.ExecuteNonQueryAsync();

            // Process children recursively
            var contentChildren = node.ChildNodes
                .Where(n => !string.IsNullOrWhiteSpace(n.InnerText) && 
                           IsContentTag(n.Name) && 
                           !string.IsNullOrWhiteSpace(n.InnerText.Trim()))
                .ToList();

            for (int i = 0; i < contentChildren.Count; i++)
            {
                var child = contentChildren[i];
                var childSequence = $"{sequence}.{i + 1}";
                await SaveNodeRecursively(connection, transaction, pageId, child, childSequence, level + 1);
            }
        }

        static void RemoveStylingTags(HtmlDocument htmlDoc)
        {
            var stylingTags = new[] { "b", "i", "strong", "em", "u", "s", "small", "mark", "del", "ins", "sup", "sub" };
            
            foreach (var tag in stylingTags)
            {
                var nodes = htmlDoc.DocumentNode.SelectNodes($"//{tag}")?.ToList();
                if (nodes != null)
                {
                    foreach (var node in nodes)
                    {
                        // Replace styling tag with its text content
                        node.ParentNode.ReplaceChild(HtmlNode.CreateNode(node.InnerHtml), node);
                    }
                }
            }
        }

        static bool IsContentTag(string tagName)
        {
            var contentTags = new HashSet<string> 
            { 
                "div", "span", "p", "h1", "h2", "h3", "h4", "h5", "h6", 
                "a", "td", "th", "li", "ul", "ol", "table", "tr", "section", 
                "article", "header", "footer", "nav", "main", "aside", "figure", 
                "figcaption", "blockquote", "code", "pre"
            };
            
            return contentTags.Contains(tagName.ToLower());
        }

        static string? GetAbsoluteUrl(string baseUrl, string relativeUrl)
        {
            try
            {
                if (Uri.TryCreate(relativeUrl, UriKind.Absolute, out Uri? absoluteUri))
                    return absoluteUri.ToString();

                if (Uri.TryCreate(new Uri(baseUrl), relativeUrl, out Uri? resultUri))
                    return resultUri.ToString();
            }
            catch { }
            return null;
        }

        static bool IsValidUrl(string url)
        {
            return Uri.TryCreate(url, UriKind.Absolute, out Uri? uriResult) 
                && (uriResult.Scheme == Uri.UriSchemeHttp || uriResult.Scheme == Uri.UriSchemeHttps);
        }

        static string NormalizeUrl(string url)
        {
            return new Uri(url).GetLeftPart(UriPartial.Path).ToLower();
        }
    }

    public class PageData
    {
        public string? Url { get; set; }
        public string? Title { get; set; }
        public string? HtmlContent { get; set; }
        public int StatusCode { get; set; }
        public string? ContentType { get; set; }
        public DateTime? LastModified { get; set; }
        public DateTime CrawledDate { get; set; }
    }

    public class CrawlItem
    {
        public string? Url { get; set; }
        public int Depth { get; set; }
    }
}