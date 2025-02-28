<?php
function enqueue_motor_thumbnail_news_css()
{
    global $post;
    if (isset($post->post_content) && has_shortcode($post->post_content, 'first_motor_thumbnail_news')) {
        wp_enqueue_style(
            'thumbnail-news-style',
            get_stylesheet_directory_uri() . '/widget-shortcodes/home/motor/css/single-thumbnail-news.css',
            array(),
            '1.0',
            'all'
        );
    }
}
add_action('wp_enqueue_scripts', 'enqueue_motor_thumbnail_news_css');

function first_motor_thumbnail_news_shortcode()
{
	$news_post = get_motor_single_thumbnail_news_data('motorcycle-news');
	
    if (!empty($news_post)) {
        ob_start();
?>
        <div class="news-post-thumbnail">
            <?php if ($news_post['thumbnail_url']): ?>
                <a href="<?php echo esc_url($news_post['link']); ?>" class="thumbnail-link">
                    <div class="thumbnail-overlay" style="position: relative;">
                        <img src="<?php echo esc_url($news_post['thumbnail_url']); ?>" alt="<?php echo esc_attr($news_post['title']); ?>"
                            style="height: 500px; width: 100%; display: block;" />

                        <span class="dd"><?php echo esc_html($news_post['news_category']); ?></span>

                        <a class="thumbnail-title"><?php echo esc_html($news_post['title']); ?></a>
                    </div>
                </a>
            <?php else: ?>
                <p>No thumbnail available.</p>
            <?php endif; ?>
        </div>
<?php

        wp_reset_postdata();

        return ob_get_clean();
    } else {
        return '<p>No news posts available.</p>';
    }
}


add_shortcode('first_motor_thumbnail_news', 'first_motor_thumbnail_news_shortcode');
