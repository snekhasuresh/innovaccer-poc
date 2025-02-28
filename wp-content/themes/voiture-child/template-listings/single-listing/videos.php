<?php
function enqueue_single_listing_video_carousal_css()
{

    wp_enqueue_style('slick-carousel', 'https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick.min.css', [], '1.8.1');
    wp_enqueue_style('slick-theme', 'https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick-theme.min.css', [], '1.8.1');
    wp_enqueue_script('slick-carousel', 'https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick.min.js', ['jquery'], '1.8.1', true);
    wp_enqueue_style('single-listing-videos-carousal', get_stylesheet_directory_uri() . '/template-listings/single-listing/css/single-listing-videos.css');
}

function single_listing_videos()
{
    enqueue_single_listing_video_carousal_css();

    $global_listing_post_data = get_listing_from_query_vars();
    $post = $global_listing_post_data['post'];
    $post_title = $post->post_title;

    $args = array(
        'post_type'      => 'video',
        'orderby'        => 'date',
        'order'          => 'DESC',
    );
    $meta_query[] = [
        'key'     => 'related_car_model',
        'value'   => '"' . $post->ID . '"',
        'compare' => 'LIKE'
    ];

    $args['meta_query'] = $meta_query;

    $video_query = new WP_Query($args);

    if (!$video_query->have_posts()) {
        return;
    }

    $video_ids = wp_list_pluck($video_query->posts, 'ID');
    $required_meta = get_selected_meta_data_for_posts($video_ids, ['video_youtube_id']);

    foreach ($video_query->posts as $post) {
        $post->video_youtube_id = $required_meta[$post->ID]['video_youtube_id'][0];
    }

?>
    <div class="variants-videos-title-con">
        <h2 class="variants-videos-title wa-title-text"><?php esc_html_e('วีดีโอคลิป '.$post_title, 'voiture'); ?></h2>
    </div>
    <div class="single-listing-car-videos-container single-listing-carousel">
        <?php foreach ($video_query->posts as $post) : ?>
            <?php
            $video_id = $post->video_youtube_id;
            $video_title = $post->post_title;
            ?>
            <div class="single-listing-car-video" style="background-color: white; padding: 10px; margin-bottom: 20px; border: 1px solid #ccc; border-radius: 5px; width:321px;">
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
        <?php endforeach; ?>
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
                    },
                    {
                        breakpoint: 480,
                        settings: {
                            slidesToShow: 1.2,
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

    do_action('wp-cardealer-single-listing-description', $post);
}

add_shortcode('single_listing_videos', 'single_listing_videos');
