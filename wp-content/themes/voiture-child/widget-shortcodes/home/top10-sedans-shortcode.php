<?php
function enqueue_top_10_sedan_css()
{
    wp_enqueue_style('slick-carousel', 'https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick.min.css', [], '1.8.1');
    wp_enqueue_style('slick-theme', 'https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick-theme.min.css', [], '1.8.1');
    wp_enqueue_script('slick-carousel', 'https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick.min.js', ['jquery'], '1.8.1', true);
    global $post;
//     if (isset($post->post_content) && has_shortcode($post->post_content, 'top10_sedan_cars')) {
        wp_enqueue_style(
            'top-10-sedan-style',
            get_stylesheet_directory_uri() . '/widget-shortcodes/home/css/top-10-sedan-cars.css',
            array(),
            '1.0',
            'all'
        );
//     }
}
add_action('wp_enqueue_scripts', 'enqueue_top_10_sedan_css');
function initialize_slick_slider()
{
?>
    <script type="text/javascript">
        jQuery(document).ready(function($) {
            setTimeout(function() {
                $('.top10-sedan-cars-list').slick({
                    slidesToShow: 4, // Shows 4 cars initially
                    slidesToScroll: 1,
                    arrows: true,
                    prevArrow: '<button class="slick-prev" aria-label="Previous" type="button">‹</button>',
                    nextArrow: '<button class="slick-next" aria-label="Next" type="button">›</button>',
                    infinite: false, // Prevents looping
                    cssEase: 'ease', // Smooth scrolling
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
                                slidesToShow: 1.2,
                                slidesToScroll: 1
                            }
                        },
                    ]
                });
            }, 500); // Delay of 500ms (adjust as needed)
        });
    </script>
<?php
}
add_action('wp_footer', 'initialize_slick_slider');


function top10_sedan_cars_shortcode($atts)
{
	enqueue_top_10_sedan_css();
    $sedan_cars = get_top_10_sedan_data($atts);
    // Start output buffering
    ob_start();
?>
    <h2 class="wa-title-text">Top 10 Mobil SUV</h2>
    <?php if (!empty($sedan_cars)) : ?>
        <div class="top10-sedan-cars-carousel-home">
            <div class="top10-sedan-cars-carousel">
                <ul class="top10-sedan-cars-list">
                    <?php foreach ($sedan_cars as $car) : ?>
                        <li class="car-sedan-item" onclick="window.location.href='<?php echo get_permalink($car['id']); ?>';">
                            <span class="badge" style="background-color: <?php echo $car['state']['color']; ?>;"><?php echo $car['state']['label']; ?></span>
                            <div class="car-thumbnail">
                                <img src="<?php echo esc_url($car['thumbnail_url'] ?: CAR_PLACEHOLDER); ?>"
                                    alt="<?php echo esc_attr($car['title']); ?>">
                            </div>
                            <div class="car-content">
                                <div class="car-info">
                                    <p class="car-make"><?php echo esc_html($car['make_name']); ?></p>
                                </div>
                                <a href="<?php echo get_permalink($car['id']); ?>" class="car-title"><?php echo esc_html($car['title']); ?></a>
                                <span class="price"><?php echo esc_html($car['price_range']); ?></span>
                                <a href="<?php echo get_permalink($car['id']); ?>" class="view-model-button">View Model</a>
                            </div>
                        </li>
                    <?php endforeach; ?>
                </ul>
            </div>
        </div>
    <?php else : ?>
        <p>No sedan cars available.</p>
<?php endif;

    return ob_get_clean();
}

add_shortcode('top10_sedan_cars', 'top10_sedan_cars_shortcode');
