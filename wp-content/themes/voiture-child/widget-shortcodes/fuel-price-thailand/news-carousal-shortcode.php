<?php
function enqueue_fuel_news_css()
{
    wp_enqueue_style('slick-carousel', 'https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick.min.css', [], '1.8.1');
    wp_enqueue_style('slick-theme', 'https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick-theme.min.css', [], '1.8.1');
    wp_enqueue_script('slick-carousel', 'https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick.min.js', ['jquery'], '1.8.1', true);
    wp_enqueue_style('fuel-news-style', get_stylesheet_directory_uri() . '/widget-shortcodes/fuel-price-thailand/css/news-carousal-shortcode.css');
}
function latest_news_fuel_shortcode($atts)
{
    enqueue_fuel_news_css();
    $atts = shortcode_atts(array(
        'brand_id' => '',
    ), $atts);

    $brand_id = $atts['brand_id'];

    // Prepare the cache key based on the brand ID
    $cache_key = 'fuel_latest_news';
    $news_posts_data = get_transient($cache_key); // Try to get cached data

    // If no cache is available, fetch the data
    if (!$news_posts_data) {
        $listing_args = array(
            'post_type'      => 'listing',
            'posts_per_page' => -1,
            'meta_query'     => array(
                array(
                    'key'     => '_listing_make',
                    'value'   => $brand_id,
                    'compare' => '='
                )
            )
        );
        $listings = get_posts($listing_args);

        $listing_ids = wp_list_pluck($listings, 'ID');
        $meta_query = array('relation' => 'OR');

        foreach ($listing_ids as $listing_id) {
            $meta_query[] = array(
                'key'     => 'related_car_models',
                'value'   => '"' . $listing_id . '"',
                'compare' => 'LIKE'
            );
        }

        $args = array(
            'post_type'      => 'news',
            'posts_per_page' => 6,
            'orderby'        => 'date',
            'order'          => 'DESC',
            'meta_query'     => $meta_query,
        );

        $news_posts = new WP_Query($args);
        $news_posts_data = [];

        while ($news_posts->have_posts()) {
            $news_posts->the_post();
            $thumbnail_id = get_post_thumbnail_id();
            $image_url = wp_get_attachment_image_src($thumbnail_id, 'full')[0];

            // Get the GUID for the image
            $thumbnail_post = get_post($thumbnail_id);
            $guid = $thumbnail_post->guid;

            $news_href = get_permalink();
            $description = wp_trim_words(get_the_content(), 20, '...');

            $news_posts_data[] = [
                'title' => get_the_title(),
                'date' => get_the_date(),
                'guid' => $guid,
                'href' => $news_href,
                'desc' => $description,
            ];
        }

        wp_reset_postdata();

        // Cache the results for 12 hours
        set_transient($cache_key, $news_posts_data, 12 * HOUR_IN_SECONDS);
    }
    // Start output buffering
    ob_start(); ?>

    <div id="fuel-latest-news-section" class="fuel-news-section inner">
        <h2 class="wa-title-text"><?php esc_html_e('Tin tức về tiêu hao nhiên liệu', 'voiture'); ?></h2>
        <div class="fuel-news-cards">
            <?php foreach ($news_posts_data as $news_post) { ?>
                <div class="fuel-news-card">
                    <img src="<?php echo esc_url($news_post['guid']); ?>" alt="News Image">
                    <div class="fuel-news-content">
                        <a class="fuel-news-title"><?php echo esc_html($news_post['title']); ?></a>
                        <p class="fuel-news-description"><?php echo esc_html($news_post['desc']); ?></p>
                        <div class="fuel-news-footer">
                            <p class="fuel-date"><?php echo esc_html($news_post['date']); ?></p>
                            <a href='<?php echo esc_url($news_post['href']); ?>' class='fuel-read-more'> Đọc thêm</a>
                        </div>
                    </div>
                </div>
            <?php } ?>
        </div>

        <div class="fuel-btn-more-container">
            <button class="fuel-btn-more">Xem thêm
                <svg width="13" height="13" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 320 512">
                    <path d="M310.6 233.4c12.5 12.5 12.5 32.8 0 45.3l-192 192c-12.5 12.5-32.8 12.5-45.3 0s-12.5-32.8 0-45.3L242.7 256 73.4 86.6c-12.5-12.5-12.5-32.8 0-45.3s32.8-12.5 45.3 0l192 192z" />
                </svg>
            </button>
        </div>

        <style>

        </style>
        <script>
            jQuery(document).ready(function($) {
                var $carousel = $('.fuel-news-cards');

                var slidesCount = $carousel.find('.fuel-news-card').length;

                $carousel.slick({
                    slidesToShow: Math.min(3, slidesCount), // Show as many slides as available, max 3
                    slidesToScroll: 1,
                    arrows: true,
                    prevArrow: '<button class="slick-prev" aria-label="Previous" type="button">‹</button>',
                    nextArrow: '<button class="slick-next" aria-label="Next" type="button">›</button>',
                    infinite: slidesCount > 3, // Infinite scroll only when there are more than 3 slides
                    responsive: [{
                            breakpoint: 1024,
                            settings: {
                                slidesToShow: Math.min(2, slidesCount), // Adjust for smaller screens
                                slidesToScroll: 1
                            }
                        },
                        {
                            breakpoint: 600,
                            settings: {
                                slidesToShow: 1,
                                slidesToScroll: 1
                            }
                        }
                    ]
                });
            });
        </script>

    </div>

<?php
    return ob_get_clean();
}
add_shortcode('fuel_latest_news', 'latest_news_fuel_shortcode');
