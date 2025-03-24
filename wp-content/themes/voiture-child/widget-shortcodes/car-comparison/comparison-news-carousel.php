<?php
function enqueue_comparison_news_css()
{
    wp_enqueue_style('slick-carousel', 'https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick.min.css', [], '1.8.1');
    wp_enqueue_style('slick-theme', 'https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick-theme.min.css', [], '1.8.1');
    wp_enqueue_script('slick-carousel', 'https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick.min.js', ['jquery'], '1.8.1', true);
    wp_enqueue_style('ev-tech-news-style', get_stylesheet_directory_uri() . '/widget-shortcodes/ev/css/ev-tech-news.css');
}
// add_action('wp_enqueue_scripts', 'enqueue_comparison_news_css');

function comaprison_news($atts)
{
    enqueue_comparison_news_css();
    $atts = shortcode_atts(
        array(
            'category' => 'Đánh giá',
            'is_ev' => '1',
            'posts_per_page' => 5,
        ),
        $atts,
        'display_cate_news_vertically'
    );

    $news_category = $atts['category'];

    $news_posts = get_category_news_data($news_category);

    ob_start(); ?>

    <h2 class="wa-title-text">Đánh Giá So Sánh Xe Ô Tô</h2>
    <div id="fuel-latest-news-section" class="fuel-news-section inner">
        <div class="fuel-news-cards">
            <?php foreach ($news_posts as $news_post) {
                $title = $news_post['title'];
                $news_content = $news_post['content'];
                $guid = $news_post['thumbnail_url'];
                $news_href = $news_post['link'];
                $publish_time = $news_post['post_date'];

                $publish_time = date('d M, Y', strtotime($publish_time));
                $description = wp_trim_words($news_content, 20, '...'); // Trimming the description to 20 words
            ?>

                <div class="fuel-news-card" onclick="window.location.href='<?php echo esc_url($news_href); ?>';">
                    <img src="<?php echo esc_url($guid); ?>" alt="News Image">

                    <div class="fuel-news-content">
                        <a href='<?php echo esc_url($news_href); ?>' class="fuel-news-title"><?php echo esc_html($title); ?></a>
                        <p class="fuel-news-description"><?php echo esc_html($description); ?></p>
                        <div class="fuel-news-footer">
                            <p class="fuel-date"><?php echo $publish_time; ?></p>
                            <a href='<?php echo esc_url($news_href); ?>' class='fuel-read-more'>Đọc thêm</a>
                        </div>
                    </div>
                </div>

            <?php }
            wp_reset_postdata(); ?>
        </div>

        <style>
			.fuel-news-title {
    font-size: 16px;
    font-weight: bold;
    margin-bottom: 10px;
    display: -webkit-box;
    -webkit-line-clamp: 2;
    -webkit-box-orient: vertical;
    overflow: hidden;
    text-overflow: ellipsis;
    font-family: 'Roboto';
    line-height: 20px;
    color: #262626;
}
            .fuel-news-description {
                font-size: 12px;
                color: #8c8c8c;
                display: -webkit-box;
                -webkit-line-clamp: 2;
                -webkit-box-orient: vertical;
                overflow: hidden;
                line-height: 1.3em;
                font-family: 'Roboto';
                text-overflow: ellipsis;
            }

            .fuel-news-footer .fuel-date {
                font-size: 12px;
                color: #8c8c8c;
                font-weight: 500;
                font-family: 'Roboto';
            }

            .fuel-read-more {
                font-size: 14px;
                color: #576b95;
                text-decoration: none;
                font-weight: 700;
                font-family: 'Roboto';
            }

            .fuel-news-section .slick-prev {
                background-color: #ffffff !important;
                border-radius: 50%;
                width: 50px;
                height: 50px;
                z-index: 10;
                position: absolute;
                top: 42% !important;
                transform: translateY(-50%);
                display: flex;
                justify-content: center;
                align-items: center;
                box-shadow: 0 2px 5px 0 rgba(0, 0, 0, 0.15);
                transition: background-color 0.3s ease, color 0.3s ease;
                border: none;
            }

            .fuel-news-section .slick-next {
                background-color: #ffffff !important;
                border-radius: 50%;
                width: 50px;
                height: 50px;
                z-index: 10;
                position: absolute;
                top: 42% !important;
                transform: translateY(-50%);
                display: flex;
                justify-content: center;
                align-items: center;
                box-shadow: 0 2px 5px 0 rgba(0, 0, 0, 0.15);
                transition: background-color 0.3s ease, color 0.3s ease;
                border: none;
            }

            .slick-prev {
                left: -2px !important;
            }

            .fuel-news-section .slick-next {
                right: -13px !important;
            }

            @media screen and (max-width: 768px) {
                #fuel-latest-news-section .slick-prev {
                    display: none !important;
                }

                #fuel-latest-news-section .slick-next {
                    display: none !important;
                }
            }
        </style>
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

    <div style="display: flex; justify-content: center;">
        <a href="<?php echo esc_url(home_url('/news')); ?>" id="view-more-btn" style="background-color: white; border:none" class="button">
            Xem thêm <span class="icon">&#10095;</span>
        </a>
    </div>

<?php
    return ob_get_clean();
}
add_shortcode('comaprison_news', 'comaprison_news');
