<?php
function latest_news_shortcode($atts)
{
    $all_news = get_latest_news_data('news');
    $news = array_slice($all_news, 0, 5);

    ob_start();
?>
    <!-- Your HTML Structure for Related News -->
    <h2 style="margin-bottom: 16px; " class="wa-title-text">Latest News</h2>
    <div id="buying-guide-news-container" style="list-style: none; padding: 0;">
        <?php foreach ($all_news as $news): ?>
            <?php
            $title = $news['title'];
            $guid = $news['thumbnail_url'];
            $custom_link = $news['link'];
            $author_name = $news['author'];
            $post_date = $news['post_date'];
            ?>
            <div class="buying-guide-news-item">
                <!-- Post Thumbnail -->
                <div class="news-thumbnail">
                    <a href="<?php echo $custom_link; ?>">
                        <img src="<?php echo esc_url($guid); ?>" alt="<?php echo $title; ?>" style="width: 110px; border-radius: 5px; height:74px;">
                    </a>
                </div>
                <!-- Post Info -->
                <div class="news-info" style="flex: 1;">
                    <h3 style="font-size: 14px; font-weight: bold; color: #262626; margin: 0; overflow: hidden; display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical;font-weight:700;">
                        <a href="<?php echo $custom_link; ?>" style="color: inherit; text-decoration: none;"><?php echo $title; ?></a>
                    </h3>

                    <div class="news-meta-author-page">
                        <?php echo $author_name; ?> • <?php echo $post_date; ?>
                    </div>
                </div>
            </div>
        <?php endforeach; ?>
    </div>

    <!-- "View More" Button -->
    <div id="view-more-container" style="text-align: center; margin-top: 20px;">
        <a href="<?php echo home_url('/news/latest'); ?>" class="view-more">
            View More
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7" />
            </svg>
        </a>
    </div>

    <style>
        .news-meta-author-page {
            font-family: "Roboto";
            font-weight: 400;
            font-size: 12px;
            color: #8c8c8c;
            letter-spacing: 0px;
            line-height: 33px;
        }


        .buying-guide-news-item {
            display: flex;
            padding: 6px 0;
            border-bottom: 1px solid #e0e0e0;
            gap: 15px;
        }

        .news-info {
            color: #262626;
            font-size: 14px;
            font-weight: 700;
            font-family: "Roboto";
            line-height: 20px;
            overflow: hidden;
            text-overflow: ellipsis;
            display: -webkit-box;
            -webkit-line-clamp: 2;
            -webkit-box-orient: vertical;
        }

        .view-more {
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 5px;
            margin: 24px auto 0;
            padding: 8px 16px;
            border: none;
            background: none;
            color: #576B95;
            font-size: 16px;
            font-weight: 700;
            cursor: pointer;
        }

        .view-more svg {
            width: 18px;
            height: 20px;
        }
    </style>
<?php
    wp_reset_postdata();
    return ob_get_clean();
}
add_shortcode('latest_news_shortcode', 'latest_news_shortcode');
