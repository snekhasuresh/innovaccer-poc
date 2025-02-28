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
    $news_post = get_single_thumbnail_news_data('news');

    if (!empty($news_post)) {
        ob_start();
?>
        <div class="news-post-thumbnail">
            <?php if ($news_post['thumbnail_url']): ?>
                <a href="<?php echo esc_url($news_post['link']); ?>" class="thumbnail-link">
                    <div class="thumbnail-overlay" style="position: relative;">
                        <img src="<?php echo esc_url($news_post['thumbnail_url']); ?>" alt="<?php echo esc_attr($news_post['title']); ?>"
                            class="thumbnail-image" />

                        <span class="dd"><?php echo esc_html($news_post['news_category']); ?></span>

                        <span class="thumbnail-title"><?php echo esc_html($news_post['title']); ?></span>
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


add_shortcode('first_news_post_with_thumbnail', 'first_news_post_with_thumbnail_shortcode');
