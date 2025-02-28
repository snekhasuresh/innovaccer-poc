<?php
function enqueue_video_carousal_css()
{
    wp_enqueue_style('slick-carousel', 'https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick.min.css', [], '1.8.1');
    wp_enqueue_style('slick-theme', 'https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick-theme.min.css', [], '1.8.1');
    wp_enqueue_script('slick-carousel', 'https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick.min.js', ['jquery'], '1.8.1', true);
    wp_enqueue_style('car-videos-carousal', get_stylesheet_directory_uri() . 'widget-shortcodes/new-cars/css/find-news-videos-carousal.css');
}

function latest_car_videos_carousal($atts)
{
    enqueue_video_carousal_css();
    // Arguments to get video posts
    $args = array(
        'post_type' => 'video',
        'posts_per_page' => 4 // Show 4 videos initially for the carousel
    );
    $video_query = new WP_Query($args);

    ob_start();
?>

    <div class="latest-car-videos-container ev-comparison-carousel">

        <?php if ($video_query->have_posts()): ?>
            <?php while ($video_query->have_posts()):
                $video_query->the_post();
                $video_id = get_post_meta(get_the_ID(), 'video_youtube_id', true);
                $video_title = get_the_title(); // Get the video title
            ?>

                <div class="latest-car-video" style="background-color: white; padding: 10px; margin-bottom: 20px; border: 1px solid #ccc; border-radius: 5px;">
                    <div class="video-overlay" style="display:flex;">
                        <iframe width="150" height="150"
                            src="https://www.youtube.com/embed/<?php echo esc_attr($video_id); ?>?controls=1&modestbranding=1&rel=0&loop=1&playlist=<?php echo esc_attr($video_id); ?>&fs=0&iv_load_policy=3"
                            frameborder="0"
                            allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture"
                            loading="lazy"
                            allowfullscreen>

                        </iframe>
                    </div>
                    <h3 class="find-latest-car-video-description">
                        <?php echo esc_html($video_title); ?>
                    </h3>
                </div>
            <?php endwhile; ?>
        <?php endif; ?>
    </div>
    <div id="videoModal" class="modal">
        <span class="close">&times;</span>
        <div class="modal-content">
            <iframe id="modal-video-iframe" width="100%" height="500px" src="" frameborder="0" allow="autoplay; encrypted-media" allowfullscreen></iframe>
        </div>
    </div>
    <script type="text/javascript">
        jQuery(document).ready(function($) {
            $('.ev-comparison-carousel').slick({
                slidesToShow: 3, // Shows 3 videos initially
                slidesToScroll: 1,
                arrows: true,
                prevArrow: '<button class="slick-prev" aria-label="Previous" type="button">‹</button>',
                nextArrow: '<button class="slick-next" aria-label="Next" type="button">›</button>',
                infinite: false, // Prevents looping
                cssEase: 'ease', // Smooth scrolling
                responsive: [{
                        breakpoint: 1024,
                        settings: {
                            slidesToShow: 3,
                            slidesToScroll: 1
                        }
                    },
                    {
                        breakpoint: 600,
                        settings: {
                            slidesToShow: 2,
                            slidesToScroll: 1
                        }
                    },
                    {
                        breakpoint: 480,
                        settings: {
                            slidesToShow: 1,
                            slidesToScroll: 1
                        }
                    }
                ]
            });
        });

        $('.video-overlay').on('click', function() {
            var videoId = $(this).data('video-id');
            var modal = $('#videoModal');
            var iframe = $('#modal-video-iframe');

            // Open modal and set video source
            iframe.attr('src', 'https://www.youtube.com/embed/' + videoId + '?autoplay=1');
            modal.show();
        });

        // Close the modal
        $('.close').on('click', function() {
            var modal = $('#videoModal');
            var iframe = $('#modal-video-iframe');

            // Stop the video
            iframe.attr('src', '');
            modal.hide();
        });

        // Close modal when clicking outside content
        $(window).on('click', function(event) {
            var modal = $('#videoModal');
            if (event.target == modal[0]) {
                var iframe = $('#modal-video-iframe');
                iframe.attr('src', '');
                modal.hide();
            }
        });
    </script>


<?php
    wp_reset_postdata(); // Reset the query
    return ob_get_clean();
}

add_shortcode('latest_car_videos_carousal', 'latest_car_videos_carousal');
