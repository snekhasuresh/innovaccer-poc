<?php
function enqueue_ev_tech_news_css()
{
    wp_enqueue_style('slick-carousel', 'https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick.min.css', [], '1.8.1');
    wp_enqueue_style('slick-theme', 'https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick-theme.min.css', [], '1.8.1');
    wp_enqueue_script('slick-carousel', 'https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick.min.js', ['jquery'], '1.8.1', true);
    wp_enqueue_style('ev-tech-news-style', get_stylesheet_directory_uri() . '/widget-shortcodes/ev/css/ev-tech-news.css');
}

function ev_technology_news($atts)
{
    $atts = shortcode_atts(
        array(
            'category' => 'Tech',
            'is_ev' => '1',
            'posts_per_page' => 5,
        ),
        $atts,
        'display_cate_news_vertically'
    );

    $news_data = get_ev_technology_news_data();
    if (empty($news_data)) {
        return;
    }

    ob_start(); ?>
    <div class="popular-ev-head">
        <h2 class="wa-title-text">EV Technology News</h2>
    </div>

    <div id="fuel-latest-news-section" class="fuel-news-section inner">
        <div class="fuel-news-cards">
            <?php foreach ($news_data as $index => $news_item) {
            ?>
                <div class="fuel-news-card">
                    <a href="<?php echo esc_url($news_item['news_href']); ?>">
                        <img src="<?php echo esc_url($news_item['guid']); ?>" alt="News Image">
                    </a>
                    <div class="fuel-news-content">

                        <a href="<?php echo esc_url($news_item['news_href']); ?>" class="fuel-news-title"><?php echo esc_html($news_item['title']); ?></a>
                        <a href="<?php echo esc_url($news_item['news_href']); ?>">
                            <p class="fuel-news-description"><?php echo esc_html($news_item['description']); ?></p>
                        </a>

                        <div class="fuel-news-footer">
                            <div class="author-and-date-con">
                                <p class="author"><?php echo esc_html($news_item['author']); ?></p>
                                <p class="fuel-date"><?php echo $news_item['publish_time']; ?></p>
                            </div>
                            <a href='<?php echo esc_url($news_item['news_href']); ?>' class='fuel-read-more'>Read More</a>
                        </div>
                    </div>
                </div>

            <?php }
            wp_reset_postdata(); ?>
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

    <a href="<?php echo esc_url(home_url('/news/evs')); ?>" id="view-more-btn" style="background-color: white; border:none; justify-content: center;color: #576b95;display: flex;font-size: 14px;font-weight: 700;" class="button">
        View More <span class="icon">&#10095;</span>
    </a>

<?php
    return ob_get_clean();
}
add_shortcode('ev_technology_news', 'ev_technology_news');
