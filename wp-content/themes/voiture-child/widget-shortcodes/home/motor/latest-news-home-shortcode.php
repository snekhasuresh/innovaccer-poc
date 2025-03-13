<?php
function enqueue_motor_latest_news_css()
{
        // Register and enqueue the CSS file
        wp_enqueue_style(
            'latest-news-style',
            get_stylesheet_directory_uri() . '/widget-shortcodes/home/motor/css/latest-news-home.css',
            array(),
            '1.0',
            'all'
        );
    }

function motor_latest_news_with_thumbnail_shortcode($atts)
{
	enqueue_motor_latest_news_css();
    $latest_news = get_motor_latest_news_data('motorcycle-news');
	
    ob_start();

    if (!empty($latest_news)) {
		$translate = [
			'Berita Terkini' => 'Tin mới nhất',
		];
?>
        <div style="display: flex; align-items: center;">
            <span>
                <h2 class="wa-title-text"> <?php echo $translate['Berita Terkini']; ?></h2>
            </span>
        </div>
        <ul class="latest-news-list">
            <?php
            foreach ($latest_news as $post_data) : ?>
                <li class="news-item">
				
                    <?php if (!empty($post_data['thumbnail_url'])) : ?>
                        <a href="<?php echo esc_url($post_data['link']); ?>" class="news-thumbnail">
                            <img src="<?php echo esc_url($post_data['thumbnail_url']); ?>" alt="<?php echo $post_data['title']; ?>" />
                        </a>
                    <?php endif; ?>
                    <div class="news-content">
                        <h3><a href="<?php echo esc_url($post_data['link']); ?>" class="news-title"><?php echo $post_data['title'] ?></a></h3>
                        <div class="news-description">
                            <?php echo esc_html($post_data['content']); ?>
                        </div>
                        <div class="news-meta">
                            <div class="news-avatar-author">
                                <a href="<?php echo esc_url($post_data['custom_author_link']); ?>">
                                    <span class="news-avatar">
                                        <img src="<?php echo esc_url($post_data['avatar']); ?>" />
                                        <span class="news-author"><?php echo esc_html($post_data['author']); ?> </span>
                                    </span>
                                </a>
                            </div>
                            <span class="news-date"><?php echo esc_html($post_data['post_date']); ?></span>
                        </div>
                    </div>
                </li>
            <?php endforeach; ?>
        </ul>
        <style>
            .news-avatar img {
                border-radius: 50% !important;
            }
        </style>
    <?php


    } else {
    ?>
        <p>No news available.</p>
<?php
    }

    wp_reset_postdata();

    return ob_get_clean();
}

add_shortcode('motor_latest_news', 'motor_latest_news_with_thumbnail_shortcode');
