document.addEventListener('DOMContentLoaded', function () {
    // Select all Edit Car buttons
    const editCarButtons = document.querySelectorAll('.edit-car-btn');
    const popup = document.getElementById('car-edit-popup');

    // Add event listener to each button
    editCarButtons.forEach(function (button) {
        button.addEventListener('click', function () {
            const carTitle = this.getAttribute('data-car-title');
            const carImage = this.getAttribute('data-car-image');
            const numberPlate = this.getAttribute('data-number-plate');
            const carVariantId = this.getAttribute('data-car-variant-id');


            // Update popup content
            popup.querySelector('.car-preview img').setAttribute('src', carImage);
            popup.querySelector('.car-preview h4').textContent = carTitle;
            popup.querySelector('#license-plate').value = numberPlate;
            
            // set data-car-variant-id attribute on the save button
            const saveButton = popup.querySelector('.edit-confirm-btn');
            saveButton.setAttribute('data-car-variant-id', carVariantId);

            // Show the popup
            popup.style.display = 'flex';
        });
    });

    // Close button in the popup
    const closeButton = document.querySelector('.close-popup');
    closeButton.addEventListener('click', function () {
        popup.style.display = 'none';
    });

    // Close the popup when clicking outside the content
    document.addEventListener('click', function (e) {
        if (e.target === popup) {
            popup.style.display = 'none';
        }
    });

    // add event listener for delete button
    const deleteButtons = document.querySelectorAll('.delete-car-btn');
    deleteButtons.forEach(function (button) {
        button.addEventListener('click', function () {
            const carId = this.getAttribute('data-car-variant-id');
            const confirmation = confirm('Are you sure you want to delete this car?');
            if (confirmation) {
                // console.log('Car ID:', carId);
                // Send jquery AJAX request to delete the car 
                jQuery.ajax({
                    url: ajax_object.ajax_url,
                    type: 'POST',
                    data: {
                        action: 'delete_car',
                        variantId: carId
                    },
                    success: function (response) {
                    if (response.success) {
                        alert('Car deleted successfully.');
                        // remove all query parameters from the URL
                        window.history.replaceState(null, '', window.location.pathname);
                        window.location.reload();
                    } else {
                        alert('Failed to delete car. Please try again.');
                    }
                    }
                });
            }
        });
    });

    // add event listener for save button
    const saveButton = document.querySelector('.edit-confirm-btn');
    saveButton.addEventListener('click', function () {
        const carId = this.getAttribute('data-car-variant-id');
        const numberPlate = document.getElementById('license-plate').value;
        // console.log('Car ID:', carId);
        // console.log('License Plate:', numberPlate);
        // Send jquery AJAX request to save the car 
        jQuery.ajax({
            url: ajax_object.ajax_url,
            type: 'POST',
            data: {
                action: 'edit_car',
                variantId: carId,
                numberPlate: numberPlate
            },
            success: function (response) {
            if (response.success) {
                window.history.replaceState(null, '', window.location.pathname);
                window.location.reload();
            } else {
                alert('Failed to save car. Please try again.');
            }
            }
        });
        })
});

