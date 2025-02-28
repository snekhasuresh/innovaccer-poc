<?php
function enqueue_single_listing_motor_video_carousal_css()
{

    wp_enqueue_style('slick-carousel', 'https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick.min.css', [], '1.8.1');
    wp_enqueue_style('slick-theme', 'https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick-theme.min.css', [], '1.8.1');
    wp_enqueue_script('slick-carousel', 'https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick.min.js', ['jquery'], '1.8.1', true);

    wp_enqueue_style('single-listing-videos-carousal', get_stylesheet_directory_uri() . '/widget-shortcodes/motor-template-listings/overview/motor/css/single-listing-videos.css');
}

function single_listing_motor_videos()
{
    enqueue_single_listing_motor_video_carousal_css();
    $make = get_query_var('make');
    $listing_name = get_query_var('model');
    $listing_name = $make . '-' . $listing_name;
    $listing_post = get_posts(array(
        'name' => $listing_name,
        'post_type' => 'motorcycle-listing',
        'posts_per_page' => 1
    ));
    $post = $listing_post[0];
    $post_data = get_post($post->ID);
    // Check if the post type is 'listing' or based on your requirements
    if ($post->post_type == 'motorcycle-listing') {

        // Your custom car video carousel logic
        $brand_id = get_post_meta($post->ID, 'make', true); // Get the brand ID from the post meta

        $args = array(
            'post_type'      => 'motocycle-video',
            'orderby'        => 'date',
            'order'          => 'DESC',
        );

        if (!empty($brand_id)) {
            $listing_ids = wp_list_pluck(get_posts([
                'post_type'      => 'motorcycle-listing',
                'posts_per_page' => -1,
                'meta_query'     => [['key' => 'make', 'value' => $brand_id, 'compare' => '=']]
            ]), 'ID');

            if (!empty($listing_ids)) {
                $meta_query = ['relation' => 'OR'];

                foreach ($listing_ids as $listing_id) {
                    $meta_query[] = [
                        'key'     => 'related_bike_model',
                        'value'   => '"' . $listing_id . '"',
                        'compare' => 'LIKE'
                    ];
                }

                // Add meta_query to args only if listings exist
                $args['meta_query'] = $meta_query;
            }
        }

        $video_query = new WP_Query($args);

?>
        <?php if ($video_query->have_posts()) : ?>
            <div class="variants-videos-title-con">
                <h2 class="variants-videos-title"><?php esc_html_e($post_data->post_title . ' Videos', 'voiture'); ?></h2>

            </div> <?php endif; ?>
        <div class="single-listing-car-videos-container single-listing-carousel">
            <?php if ($video_query->have_posts()) : ?>
                <?php while ($video_query->have_posts()) : $video_query->the_post(); ?>
                    <?php
                    $video_id = get_post_meta(get_the_ID(), 'video_youtube_id', true);
                    $video_title = get_the_title(); // Get the video title
                    ?>
                    <div class="single-listing-car-video" style="background-color: white; padding: 10px; margin-bottom: 20px; border: 1px solid #ccc; border-radius: 5px;">
                        <div class="video-overlay" style="display:flex;">
                            <iframe width="100%" height="150"
                                src="https://www.youtube.com/embed/<?php echo esc_attr($video_id); ?>?controls=1&modestbranding=1&rel=0&loop=1&playlist=<?php echo esc_attr($video_id); ?>&fs=0&iv_load_policy=3"
                                frameborder="0"
                                allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture"
                                allowfullscreen>
                            </iframe>
                        </div>
                        <div>
                            <h3 class="single-listing-video-title">
                                <?php echo esc_html($video_title); ?>
                            </h3>
                        </div>
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
                $('.single-listing-carousel').slick({
                    slidesToShow: 3,
                    slidesToScroll: 1,
                    arrows: true,
                    prevArrow: '<button class="slick-prev" aria-label="Previous" type="button">‹</button>',
                    nextArrow: '<button class="slick-next" aria-label="Next" type="button">›</button>',
                    infinite: false,
                    cssEase: 'ease',
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

                // Video overlay click handler
                $('.video-overlay').on('click', function() {
                    var videoId = $(this).data('video-id');
                    var modal = $('#videoModal');
                    var iframe = $('#modal-video-iframe');
                    iframe.attr('src', 'https://www.youtube.com/embed/' + videoId + '?autoplay=1');
                    modal.show();
                });

                // Close the modal
                $('.close').on('click', function() {
                    var modal = $('#videoModal');
                    var iframe = $('#modal-video-iframe');
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
            });
        </script>



    <?php
        wp_reset_postdata();
    }
    ?>

<?php do_action('wp-cardealer-single-listing-description', $post);
}
add_shortcode('single_listing_motor_videos', 'single_listing_motor_videos');
?>