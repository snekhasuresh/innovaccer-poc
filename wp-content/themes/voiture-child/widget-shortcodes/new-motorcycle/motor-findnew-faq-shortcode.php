<?php
include_once get_stylesheet_directory() . '/json-ld/faq-json-ld.php';

function enqueue_find_new_bike_faq_css()
{
    wp_enqueue_style('find-new-cars-faq-style', get_stylesheet_directory_uri() . '/widget-shortcodes/new-cars/css/find-new-cars-faq.css', array(), '1.0', 'all');
}
// add_action('wp_enqueue_scripts', 'enqueue_find_new_car_faq_css');

function findnew_bike_faqs_shortcode($atts)
{
    enqueue_find_new_bike_faq_css();
    $atts = shortcode_atts(array(
        'brand_id' => '',
    ), $atts);

    $brand_name = get_query_var('make');

    $faq_posts_data = get_motor_faq_data($brand_name);
    $faq_posts = $faq_posts_data['posts'];
    $brand_name = $faq_posts_data['brand_name'];

    if ($faq_posts === null) {
        return '';
    }

    $json_ld_faqs = [];
    foreach ($faq_posts as $faq_post) {
        $post_id = $faq_post->ID;
        $meta_data = get_post_meta($post_id);
        $question = isset($meta_data['question'][0]) ? esc_html($meta_data['question'][0]) : 'No question available';
        $answer = isset($meta_data['answer'][0]) ? wp_kses_post($meta_data['answer'][0]) : 'No answer available';

        $json_ld_faqs[] = array(
            'id' => $post_id,
            'question' => $question,
            'answer' => $answer,
        );
    }
    wp_reset_postdata();

    add_faq_json_ld($json_ld_faqs);

    ob_start();
	$title = !empty($brand_name) ? esc_html('คำถามที่พบบ่อยเกี่ยวกับรถมอเตอร์ไซค์ ' . $brand_name . ' ' . date('Y')) : 'คำถามที่พบบ่อยเกี่ยวกับรถมอเตอร์ไซค์';
    echo "<h2 class='wa-title-text find-new-faq-title' >$title</h2>";
?>
    <div id="listing-detail-description " class="description inner find-new-faq-con">
        <div class="description-inner">
            <div class="description-inner-wrapper">
                <div class="accordion">
                    <div class="acc">
                        <?php
                        foreach ($json_ld_faqs as $json_ld_faq) {
                            $post_id = $json_ld_faq['id'];
                            $question = $json_ld_faq['question'];
                            $answer = $json_ld_faq['answer'];

                            echo '<div class="accordion-item">';
                            echo '<input type="checkbox" id="faq-question-' . $post_id . '">';
                            echo '<label for="faq-question-' . $post_id . '" class="accordion-header">' . $question . '</label>';
                            echo '<div class="accordion-content">' . $answer . '</div>';
                            echo '</div>';
                        }
                        wp_reset_postdata();
                        ?>
                    </div>
                </div>
            </div>
        </div>
    </div>
<?php

    $output = ob_get_clean(); // Get the buffered content

    return $output; // Return the content for the shortcode
}
add_shortcode('findnew_bike_faqs_shortcode', 'findnew_bike_faqs_shortcode');
