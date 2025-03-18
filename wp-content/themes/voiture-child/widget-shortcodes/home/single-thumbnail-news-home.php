<?php
function enqueue_thumbnail_news_css()
{
    global $post;
//     if (isset($post->post_content) && has_shortcode($post->post_content, 'first_news_post_with_thumbnail')) {
        wp_enqueue_style(
            'thumbnail-news-style',
            get_stylesheet_directory_uri() . '/widget-shortcodes/home/css/single-thumbnail-news.css',
            array(),
            '1.0',
            'all'
        );
//     }
}
add_action('wp_enqueue_scripts', 'enqueue_thumbnail_news_css');

function first_news_post_with_thumbnail_shortcode()
{
	enqueue_thumbnail_news_css();
    $news_posts = get_single_thumbnail_news_data('news');
	
      if (!empty($news_posts) && is_array($news_posts)) {
        ob_start();
?>
        <div class="news-posts-container" style="display: flex; gap: 20px;">
            <!-- Left Side: First Post -->
            <div class="news-post-left" style="flex: 2;">
                <?php $first_post = $news_posts[0]; ?>
                <a href="<?php echo esc_url($first_post['link']); ?>" class="thumbnail-link">
                    <div class="thumbnail-overlay" style="position: relative;">
                        <img src="<?php echo esc_url($first_post['thumbnail_url']); ?>" alt="<?php echo esc_attr($first_post['title']); ?>" style="height: 500px; width: 100%; display: block;" />
                        <span class="dd" style="position: absolute; bottom: 10px; left: 10px; background: #F5C34B; color: #FFF; padding: 5px 10px; font-weight: bold; height: 26px; width: max-content;"><?php echo esc_html($first_post['news_category']); ?></span>
                        <h2 class="thumbnail-title" style="position: absolute; bottom: 10px; left: 10px; color: #FFF; font-size: 24px;"><?php echo esc_html($first_post['title']); ?></h2>
                    </div>
                </a>
            </div>

            <!-- Right Side: Next Two Posts -->
            <div class="news-post-right" style="flex: 1; display: flex; flex-direction: column; gap: 10px;">
                <?php for ($i = 1; $i < count($news_posts); $i++): ?>
                    <a href="<?php echo esc_url($news_posts[$i]['link']); ?>" class="thumbnail-link">
                        <div class="thumbnail-overlay" style="position: relative; display: flex; flex-direction: column;">
                            <img src="<?php echo esc_url($news_posts[$i]['thumbnail_url']); ?>" alt="<?php echo esc_attr($news_posts[$i]['title']); ?>" style="height: 245px; width: 100%; display: block;" />
                            <h3 class="thumbnail-title-right" style="color: #FFF; font-size: 18px;"><?php echo esc_html($news_posts[$i]['title']); ?></h3>
                        </div>
                    </a>
                <?php endfor; ?>
            </div>
        </div>
<?php

        wp_reset_postdata();

        return ob_get_clean();
    } else {
        return '<p>No news posts available.</p>';
    }
}


add_shortcode('first_news_post_with_thumbnail', 'first_news_post_with_thumbnail_shortcode');
