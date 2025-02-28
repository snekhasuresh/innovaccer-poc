<?php

// import news.css
function single_listing_news_css()
{
    wp_enqueue_style('news', get_stylesheet_directory_uri() . '/template-listings/single-listing/css/news.css');
}

function single_listing_news_shortcode($atts)
{
    single_listing_news_css();

    ob_start(); // Start output buffering

    $global_listing_post_data = get_listing_from_query_vars();
    $car_post = $global_listing_post_data['post'];

    // WP_Query arguments to fetch related news
    $args = array(
        'post_type' => 'news',
        'posts_per_page' => 6,
        'meta_query' => array(
            array(
                'key' => 'related_car_model',
                'value' => 's:' . strlen((string) $car_post->ID) . ':"' . $car_post->ID . '";',
                'compare' => 'LIKE'
            ),
            array(
                'key'     => 'second_language',
                'value'   => '',
                'compare' => '='
            )
        )
    );
    $news_posts = new WP_Query($args);

    if (!$news_posts->have_posts()) {
        return '';
    }

    $post_ids = wp_list_pluck($news_posts->posts, 'ID');
    $all_news_meta = get_post_meta_with_thumbnail_guid($post_ids);

    $author_ids = wp_list_pluck($news_posts->posts, 'post_author');
    $author_data = get_author_data($author_ids);

    $news_post = [];
    foreach ($news_posts->posts as $post) :
        $news_id = $post->ID;
        $title = $post->post_title;
        $description = wp_trim_words($post->post_content, 20, '...');
        $author_id = $post->post_author;
        $post_meta = $all_news_meta[$news_id] ?? [];
        $thumbnail_url = $post_meta['_thumbnail_guid'] ?? '';
        $author_name  = $author_data[$author_id]['display_name'] ?? '';

        $news_post[] = [
            'id' => $news_id,
            'title' => $title,
            'link'  => get_custom_post_link($news_id, 'news'),
            'thumbnail_url' => $thumbnail_url,
            'description' => $description,
            'date' => get_the_date('d.m.Y', $news_id),
            'author' => $author_name,
        ];
    endforeach;

    if (!empty($news_post)) {

?>
        <div id="listing-detail-description" class="description inner">
            <div class="variant-news-title-con">
                <h2 class="variant-news-title wa-title-text "><?php esc_html_e('ข่าวสาร '.$car_post->post_title, 'voiture'); ?></h2>
            </div>
            <div class="news-cards">
                <?php
                foreach ($news_post as $post_data) :
                ?>
                    <div class="news-card" data-url="<?php echo $post_data['link']; ?>">
                        <?php if ($post_data['thumbnail_url']) : ?>
                            <div class="varient-news-image">
                                <img src="<?php echo esc_url($post_data['thumbnail_url']); ?>" alt="<?php echo $post_data['title']; ?>" />
                            </div>
                        <?php endif; ?>
                        <div class="news-content">
                            <p class="car-model-varient-news"><span class="car-brand-icon">•</span> <?php echo esc_html($car_post->post_title); ?></p>
                            <a class="news-title"><?php echo esc_html($post_data['title']); ?></a>
                            <p class="description"><?php echo esc_html($post_data['description']); ?></p>
                            <div class="news-footer">
                                <div class="author-date-con">
                                    <p class="author"><?php echo esc_html($post_data['author']); ?></p>
                                    <p class="date"><?php echo esc_html($post_data['date']); ?></p>
                                </div>
                                <a href='<?php echo esc_url($post_data['link']); ?>' class='read-more'>อ่านเพิ่มเติม</a>
                            </div>
                        </div>
                    </div>
                <?php endforeach; ?>
            </div>

            <?php
            // Check if there are more than 6 posts, display "View More"
            $total_posts = $news_posts->found_posts;
            if ($total_posts > 6): ?>
                <div class="btn-more-container">
                    <button class="btn-more">
                        <?php $current_page_url = get_permalink($car_post->ID); ?>
                        <a href="<?php echo esc_url($current_page_url . 'news'); ?>">
                            ดูเพิ่มเติม <svg width="13" height="13" xmlns="http://www.w3.org/2000/svg"
                                viewBox="0 0 320 512">
                                <path d="M310.6 233.4c12.5 12.5 12.5 32.8 0 45.3l-192 192c-12.5 12.5-32.8 12.5-45.3 0s-12.5-32.8 0-45.3L242.7 256 73.4 86.6c-12.5-12.5 12.5-32.8 0-45.3s32.8-12.5 45.3 0l192 192z" />
                            </svg>
                        </a>
                    </button>
                </div>
            <?php endif; ?>

            <?php do_action('wp-cardealer-single-listing-description', $car_post); ?>
            <script>
                document.addEventListener('DOMContentLoaded', function() {
                    // Attach click event to all elements with the 'news-card' class
                    document.querySelectorAll('.news-card').forEach(function(card) {
                        card.addEventListener('click', function() {
                            const url = this.getAttribute('data-url');
                            if (url) {
                                window.location.href = url;
                            }
                        });
                    });
                });
            </script>
        </div>
<?php
    }
    return ob_get_clean(); // Return the buffered content
}
add_shortcode('single_listing_car_news', 'single_listing_news_shortcode');
