<?php
function enqueue_find_new_motor_news_css()
{
    // Register and enqueue the CSS file
    wp_enqueue_style('find-new-motor-newss-style', get_stylesheet_directory_uri() . '/widget-shortcodes/new-motorcycle/css/find-new-cars-news.css', array(), '1.0', 'all');
}
add_action('wp_enqueue_scripts', 'enqueue_find_new_motor_news_css');

function motor_related_news($atts)
{
    $atts = shortcode_atts(array(
        'brand_id' => 0,
        'brand_name' => '',
        'related_listings' => '',
    ), $atts);

    $brand_id = intval($atts['brand_id']);

    $news_posts_data = get_motor_news_data($brand_id);
    $news_posts = $news_posts_data['posts'];
    $brand_name = $news_posts_data['brand_name'];

    if ($news_posts === null) {
        return '';
    }

    ob_start();

    $title = $brand_name ? esc_html('วีดีโอ รถมอเตอร์ไซค์ '. $brand_name . ' ในไทย') : 'ข่าวล่าสุดของมอเตอร์ไซค์ในประเทศไทย';
?>
    <h2 class="wa-title-text find-car-news-title" ><?php echo $title; ?></h2>
     <div id="fuel-latest-news-section" style="margin-left:10px" class="fuel-news-section inner">
        <div class="fuel-news-cards">
            <?php
            foreach ($news_posts->posts as $post) {
                $image_url = $post->thumbnail_url;
                $news_href = $post->permalink;
                $description = wp_trim_words($post->post_content, 9, '...');
                $date = $post->post_modified;
            ?>
                <div class="fuel-news-card">
                    <a href='<?php echo esc_url($news_href); ?>'>
                        <img src="<?php echo esc_url($image_url); ?>" alt="News Image">
                    </a>
                    <div class="fuel-news-content">
                        <a href='<?php echo esc_url($news_href); ?>' class="fuel-news-title">
                            <?php echo esc_html($post->post_title); ?>
                        </a>
                        <p class="fuel-news-description"><?php echo esc_html($description); ?></p>
                        <div class="fuel-news-footer">
                            <p class="fuel-date"><?php echo $date; ?></p>
                            <a href='<?php echo esc_url($news_href); ?>' class='fuel-read-more'>Read More</a>
                        </div>
                    </div>
                </div>
            <?php
                wp_reset_postdata();
            }
            ?>
        </div>

        <script>
            jQuery(document).ready(function($) {
                $('.fuel-news-cards').slick({
                    slidesToShow: 3,
                    slidesToScroll: 1,
                    arrows: true,
                    prevArrow: '<button class="slick-prev" aria-label="Previous" type="button">‹</button>',
                    nextArrow: '<button class="slick-next" aria-label="Next" type="button">›</button>',
                    infinite: false,
                    responsive: [{
                            breakpoint: 1030,
                            settings: {
                                slidesToShow: 2,
                                slidesToScroll: 1
                            }
                        },
                        {
                            breakpoint: 768,
                            settings: {
                                slidesToShow: 1.2,
                                slidesToScroll: 1
                            }
                        }
                    ]
                });
            });
        </script>
    </div>
<?php
    $result = ob_get_clean();

    return $result;
}
add_shortcode('motor_related_news', 'motor_related_news');
