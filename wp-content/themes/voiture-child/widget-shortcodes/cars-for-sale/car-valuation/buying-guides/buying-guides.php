<?php
function enqueue_car_valuation_news_css()
{
    wp_enqueue_style('slick-carousel', 'https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick.min.css', [], '1.8.1');
    wp_enqueue_style('slick-theme', 'https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick-theme.min.css', [], '1.8.1');
    wp_enqueue_script('slick-carousel', 'https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick.min.js', ['jquery'], '1.8.1', true);
    wp_enqueue_style('ev-tech-news-style', get_stylesheet_directory_uri() . '/widget-shortcodes/ev/css/ev-tech-news.css');
}
add_action('wp_enqueue_scripts', 'enqueue_car_valuation_news_css');

function car_valuation_news($atts)
{
    $atts = shortcode_atts(
        array(
            'category' => 'Buying Guide',
            'is_ev' => '1',
            'posts_per_page' => 5,
        ),
        $atts,
        'display_cate_news_vertically'
    );

    $current_page = 1; // Start on the first page
    $posts_per_page = $atts['posts_per_page'];
    $news_category = $atts['category'];
    $taxonomy = 'news-category';

    $taxonomy = 'news-category';

    global $wpdb;
    $term_name = 'Buying Guide';
    $sql = $wpdb->prepare(
        "SELECT t.* FROM {$wpdb->terms} AS t
                INNER JOIN {$wpdb->term_taxonomy} AS tt ON t.term_id = tt.term_id
                WHERE tt.taxonomy = %s
                AND t.name = %s",
        $taxonomy,
        $term_name,
    );

    $terms = $wpdb->get_results($sql);

    if (!empty($terms)) {
        $news_category_term = $terms[0];
    } else {
        echo 'No terms found.';
    }

    if ($news_category_term) {
        $news_category_term_id = $news_category_term->term_id;
        $serialized_value = ':"' . $news_category_term_id . '";';

        $query_args = [
            'post_type'  => 'news',
            'meta_query' => [
                [
                    'key'     => 'second_language',
                    'value'   => '',
                    'compare' => '=',
                ],
                [
                    'key'     => 'news_category',
                    'value'   => $serialized_value,
                    'compare' => 'LIKE',
                ],
                // publish_time has to be less than current time
                [
                    'key'     => 'publish_time',
                    'value'   => date('Y-m-d H:i:s'),
                    'compare' => '<',
                    'type'    => 'DATETIME',
                ],
                // weight time greater than current time
                // [
                //     'key'     => 'weight',
                //     'value'   => date('Y-m-d H:i:s'),
                //     'compare' => '<',
                //     'type'    => 'DATETIME',
                // ],
            ],
            'posts_per_page' => $posts_per_page,
            'paged'          => $current_page,
            // order by weight meta key descending
            // 'meta_key'       => 'weight',
            // order by publish time descending
            'meta_key'       => 'publish_time',
            'orderby'        => 'meta_value',
            'meta_type'      => 'DATETIME',
            'order'          => 'DESC',
        ];

        $news_posts = new WP_Query($query_args);
        wp_reset_postdata();
    } else {
        echo $news_category . " category not found.";
    }


    if (!$news_posts->have_posts()) {
        echo 'No posts found.';
        return;
    }

    ob_start(); ?>

    <h2>Buying Guides</h2>
    <div id="fuel-latest-news-section" class="fuel-news-section inner">
        <div class="fuel-news-cards">
            <?php while ($news_posts->have_posts()) {
                $news_posts->the_post();
                $thumbnail_id = get_post_thumbnail_id();
                $thumbnail_post = get_post($thumbnail_id);
                $guid = $thumbnail_post->guid;

                $news_href = get_permalink();
                $description = wp_trim_words(get_the_content(), 20, '...'); // Trimming the description 

                $publish_time = get_post_meta(get_the_ID(), 'publish_time', true);
                $publish_time = date('d M, Y', strtotime($publish_time));
            ?>

                <div class="fuel-news-card">
                    <img src="<?php echo esc_url($guid); ?>" alt="News Image">


                    <div class="fuel-news-content">
                        <a class="fuel-news-title"><?php echo esc_html(get_the_title()); ?></a>
                        <p class="fuel-news-description"><?php echo esc_html($description); ?></p>
                        <div class="fuel-news-footer">
                            <p class="fuel-date"><?php echo $publish_time; ?></p>
                            <a href='<?php echo esc_url($news_href); ?>' class='fuel-read-more'>Read More</a>
                        </div>
                    </div>
                </div>

            <?php }
            wp_reset_postdata(); ?>
        </div>

        <!-- <div class="fuel-btn-more-container">
            <button class="fuel-btn-more">View More
                <svg width="13" height="13" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 320 512">
                    <path d="M310.6 233.4c12.5 12.5 12.5 32.8 0 45.3l-192 192c-12.5 12.5-32.8 12.5-45.3 0s-12.5-32.8 0-45.3L242.7 256 73.4 86.6c-12.5-12.5-12.5-32.8 0-45.3s32.8-12.5 45.3 0l192 192z" />
                </svg>
            </button>
        </div> -->


        <script>
            jQuery(document).ready(function($) {
                $('.fuel-news-cards').slick({
                    slidesToShow: 4,
                    slidesToScroll: 1,
                    arrows: true,
                    prevArrow: '<button class="slick-prev" aria-label="Previous" type="button">‹</button>',
                    nextArrow: '<button class="slick-next" aria-label="Next" type="button">›</button>',
                    infinite: false,
                    responsive: [{
                            breakpoint: 1030,
                            settings: {
                                slidesToShow: 3,
                                slidesToScroll: 1
                            }
                        },
                        {
                            breakpoint: 768,
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

<!--     <a href="<?php echo esc_url(home_url('/news')); ?>" id="view-more-btn" style="background-color: white; border:none" class="button">
        View More <span class="icon">&#10095;</span>
    </a> -->
    <div class="fuel-btn-more-container">
            
				<a href="<?php echo esc_url(home_url('/news')); ?>" id="view-more-btn" style="background-color: white; border:none" class="fuel-btn-more">  View More
                <svg width="13" height="13" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 320 512">
                    <path d="M310.6 233.4c12.5 12.5 12.5 32.8 0 45.3l-192 192c-12.5 12.5-32.8 12.5-45.3 0s-12.5-32.8 0-45.3L242.7 256 73.4 86.6c-12.5-12.5-12.5-32.8 0-45.3s32.8-12.5 45.3 0l192 192z" />
                </svg>
          </a>
        </div>

<?php
    return ob_get_clean();
}
add_shortcode('car_valuation_news', 'car_valuation_news');
